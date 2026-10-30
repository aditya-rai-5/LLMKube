/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	foremanv1alpha1 "github.com/defilantech/llmkube/api/foreman/v1alpha1"
	llmkubemetrics "github.com/defilantech/llmkube/internal/metrics"
	"github.com/defilantech/llmkube/pkg/foreman/archive"
	"github.com/defilantech/llmkube/pkg/foreman/audit"
)

// claimExpiriesAnnotation tracks how many times this task has been released
// back to Pending due to a stale or absent FleetNode. The 3-strike ladder
// (>= 2 prior expiries) terminal-fails the task to bound poison loops.
const claimExpiriesAnnotation = "foreman.llmkube.dev/claim-expiries"

// claimExpiryLimit is the maximum number of prior expiries before the task is
// terminal-failed. At count >= claimExpiryLimit this would be the
// (claimExpiryLimit+1)th expiry, which we refuse.
const claimExpiryLimit = 2

// AgenticTaskReconciler is the Foreman v0.1 scheduler. It watches
// AgenticTask resources and routes each Pending task to the first Ready
// FleetNode whose advertised capability satisfies the task's
// RequiredCapability (first-fit, alphabetical-by-name for determinism).
//
// The reconciler never touches the task while the FleetAgent owns it
// (Scheduled / Running / terminal phases). The agent owns the
// Scheduled -> Running -> Succeeded|Failed transitions; the scheduler
// only owns "no phase" -> Pending and Pending -> Scheduled. Cascade
// failure from a Failed dependency is the one exception: the scheduler
// short-circuits a downstream task with phase=Failed before it ever
// reaches Scheduled.
type AgenticTaskReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// AuditNamespace is where durable audit-record ConfigMaps are written.
	// Empty means each record lands in its task's own namespace.
	AuditNamespace string

	// ArchiveDir is where terminal-task bundles are written. Empty disables
	// archival entirely: no directory is created and no work is done.
	ArchiveDir string
}

// requeueNoFit is the backoff when no FleetNode satisfies the task's
// capability today. Long enough that a busy cluster does not get spammed,
// short enough that a node coming Ready triggers dispatch within seconds
// of the next reconcile (the FleetNode watch also re-enqueues directly).
const requeueNoFit = 10 * time.Second

// requeueWaitingForDeps is the backoff while at least one dependency is
// still pre-terminal.
const requeueWaitingForDeps = 10 * time.Second

// defaultDepWaitBudget bounds the wait for an absent dependency when the task
// does not set spec.TimeoutSeconds. One hour is deliberate, not arbitrary:
// this guard measures a dependency that is ABSENT, not one that is slow. The
// planner emits every task in a batch together, so a dependency that has not
// appeared within an hour was deleted or never emitted -- waiting longer
// cannot help it appear. Without this bound an unset TimeoutSeconds leaves the
// wait unbounded and a task whose dependency vanished stays Pending forever
// (#1729).
const defaultDepWaitBudget = 1 * time.Hour

// +kubebuilder:rbac:groups=foreman.llmkube.dev,resources=agentictasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=foreman.llmkube.dev,resources=agentictasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=foreman.llmkube.dev,resources=agentictasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=foreman.llmkube.dev,resources=fleetnodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=foreman.llmkube.dev,resources=agents,verbs=get;list;watch
// +kubebuilder:rbac:groups=foreman.llmkube.dev,resources=modelprofiles,verbs=get;list;watch

// Reconcile drives a single AgenticTask toward Scheduled.
func (r *AgenticTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var task foremanv1alpha1.AgenticTask
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.V(1).Info("reconciling AgenticTask",
		"kind", task.Spec.Kind,
		"phase", task.Status.Phase,
		"assignedNode", task.Status.AssignedNode,
	)

	// Normalize an empty phase to Pending. We do this on a fresh-from-
	// the-API view, so the next reconcile sees Pending and falls into
	// the scheduling branch.
	if task.Status.Phase == "" {
		return r.setInitialPending(ctx, &task)
	}

	// Terminal phases are done; record the durable audit entry once
	// (best-effort: a failed audit write must not wedge reconciliation),
	// then nothing left to do.
	if task.Status.Phase == foremanv1alpha1.AgenticTaskPhaseSucceeded ||
		task.Status.Phase == foremanv1alpha1.AgenticTaskPhaseFailed {
		// Emit the operator-side outcome metric exactly once, gated on the
		// audited annotation actually being PERSISTED by RecordTerminal
		// (#1491). RecordTerminal returns nil in two cases: it just wrote the
		// record and stamped the annotation (first terminal reconcile), or the
		// annotation was already set and it was a no-op (a later reconcile of
		// an already-counted task). We only emit on the first case — the
		// annotation was unset on entry AND RecordTerminal succeeded — so a
		// failed audit write means no metric rather than a metric that a later
		// reconcile (periodic resync or any update) would pass the guard and
		// repeat. Under-counting on that rare failure path is the better
		// trade: rates stay correct, whereas a repeated increment would inflate
		// exactly the agents that are having trouble.
		firstTerminal := !audit.IsRecorded(&task)
		if firstTerminal {
			if err := audit.RecordTerminal(ctx, r.Client, &task, r.AuditNamespace, log); err != nil {
				// The dedup marker was not persisted, so the metric must not
				// be emitted either: a later reconcile will retry both.
				log.Error(err, "audit: failed to record terminal run; skipping outcome metric (continuing)")
			} else {
				agent, kind, verdict, outcome, elapsedSec, turns := taskOutcomeLabels(&task)
				llmkubemetrics.RecordTaskOutcome(agent, kind, verdict, outcome, elapsedSec, turns)
			}
		}
		// Release the node reservation so the scheduler can dispatch the next
		// task there. Guarded on taskKey, so a node already reserved for a
		// different task is untouched.
		//
		// This runs BEFORE archival, and the order is load-bearing. Archival
		// writes to a mounted volume and takes no timeout: a hung NFS or CSI
		// mount blocks os.MkdirAll indefinitely. This controller runs with
		// concurrency 1, so an archive stalled ahead of the release would hold
		// the node reserved for a task that has already finished AND wedge
		// every other AgenticTask reconcile behind it. Releasing first bounds
		// a stalled mount to "no bundles are being written" instead of "the
		// fleet stops scheduling".
		if err := r.clearNodeCurrentTask(ctx, task.Status.AssignedNode, taskKey(&task)); err != nil {
			log.Error(err, "failed to release node reservation on terminal task",
				"node", task.Status.AssignedNode, "task", task.Name)
		}
		r.archiveTerminalTask(ctx, &task, log)
		return ctrl.Result{}, nil
	}

	// In-flight phases (Scheduled / Running / Verifying) with an AssignedNode
	// need claim-expiry checking: if the node is gone or stale the task must
	// be released back to Pending so the scheduler can re-dispatch it.
	if task.Status.Phase != foremanv1alpha1.AgenticTaskPhasePending &&
		task.Status.AssignedNode != "" {
		return r.checkClaimExpiry(ctx, &task)
	}

	// For non-Pending phases with no assigned node (unexpected but safe to
	// ignore), and for phases not handled above, do nothing.
	if task.Status.Phase != foremanv1alpha1.AgenticTaskPhasePending {
		return ctrl.Result{}, nil
	}

	// Cascade-skip if any dependency ended ALREADY-RESOLVED or was itself
	// Skipped (#970, transitive #1688). The dependent cannot meaningfully
	// run (the work is already on the branch, or an upstream stage had
	// nothing to do); mark it Skipped (Phase=Succeeded + Verdict=Skipped)
	// so the rollup excludes it from every bucket instead of cascade-failing
	// it (which would land at Phase=Failed and pin the Workload to Failed).
	// The skip check runs before the cascade-fail check so a resolved/skipped
	// dep never trips the "terminal without success" branch.
	if depName, depState, err := r.cascadeSkipIfDepResolvedOrSkipped(ctx, &task); err != nil {
		return ctrl.Result{}, err
	} else if depName != "" {
		return r.skipTask(ctx, &task, depName, depState, false)
	}

	// Cascade-skip if any dependency ended with a cross-stage contradiction.
	// An ALREADY-RESOLVED dependency is a terminal non-failure (the work is
	// already on the branch), while a contradicted dependency is an unresolved
	// question: two independent witnesses disagreeing is the strongest evidence
	// that something is wrong, so shipping work built on it is exactly the
	// failure the detection was built to prevent. The contradiction check runs
	// AFTER the already-resolved check so a dep that is both resolved and
	// contradicted stays skipped as already-resolved (the benign signal wins
	// over the stop signal), and a contradicted dep is skipped rather than
	// cascade-failed so the rollup excludes it from every bucket instead of
	// pinning the Workload to Failed.
	if depName, contradiction, err := r.cascadeSkipIfDepContradicted(ctx, &task); err != nil {
		return ctrl.Result{}, err
	} else if depName != "" {
		return r.skipTask(ctx, &task, depName, fmt.Sprintf("contradiction: %s", contradiction), true)
	}

	// Cascade-fail if any dependency is still absent once the task's
	// TimeoutSeconds budget has elapsed. A dep absent for less than the
	// budget is legal to wait for (the create-ordering race), so this
	// defers to allDepsSucceeded, which surfaces the wait. Runs before
	// cascadeFailIfDepFailed so the missing-dep reason is distinct from a
	// dependency that failed. See #1687.
	if missingMsg, err := r.cascadeFailIfMissing(ctx, &task); err != nil {
		return ctrl.Result{}, err
	} else if missingMsg != "" {
		return r.failTask(ctx, &task, "MissingDependency", missingMsg)
	}

	// Cascade-fail if any dependency has Failed.
	if cascadeMsg, err := r.cascadeFailIfDepFailed(ctx, &task); err != nil {
		return ctrl.Result{}, err
	} else if cascadeMsg != "" {
		return r.failTask(ctx, &task, "UpstreamFailed", cascadeMsg)
	}

	// Wait if any dependency is still pre-terminal.
	allSucceeded, err := r.allDepsSucceeded(ctx, &task)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !allSucceeded {
		return ctrl.Result{RequeueAfter: requeueWaitingForDeps}, nil
	}

	// Resolve the effective RequiredCapability. When spec.agentRef is set
	// it wins: we look up the Agent and use its capability. The task's own
	// spec.requiredCapability is ignored in that path; that is the locked
	// M3 contract. An Agent that does not exist fails the task fast.
	required, requiredModel, jobMode, err := r.effectiveRequiredCapability(ctx, &task)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.failTask(ctx, &task, "AgentNotFound",
				fmt.Sprintf("Agent %q not found in namespace %q", task.Spec.AgentRef.Name, task.Namespace))
		}
		return ctrl.Result{}, err
	}

	// Enforce the per-Agent in-flight bound. When the referenced Agent sets
	// spec.maxConcurrentTasks, count its in-flight tasks in this namespace
	// (those holding a slot: Scheduled / Running / Verifying) and decline to
	// claim an (N+1)th one; the task stays Pending for the next pass, exactly
	// as it does when no node matches. Unset = unbounded.
	if atLimit, err := r.agentAtConcurrencyLimit(ctx, &task); err != nil {
		return ctrl.Result{}, err
	} else if atLimit {
		log.Info("agent at maxConcurrentTasks; leaving task Pending", "task", task.Name)
		return ctrl.Result{RequeueAfter: requeueNoFit}, nil
	}

	// Find a Ready FleetNode that satisfies the effective RequiredCapability
	// and atomically reserve it. The reservation (stamping the node's
	// CurrentTask) is what enforces one-task-per-node and spreads work across
	// the fleet; without it every task funnels onto the first node. See #977.
	nodeName, err := r.reserveFirstFitNode(ctx, &task, required, requiredModel, jobMode)
	if err != nil {
		return ctrl.Result{}, err
	}
	if nodeName == "" {
		log.Info("no free FleetNode matches; will retry", "task", task.Name)
		return ctrl.Result{RequeueAfter: requeueNoFit}, nil
	}

	if err := r.scheduleToNode(ctx, &task, nodeName); err != nil {
		// The node is reserved but the task never reached Scheduled. Release the
		// reservation so the node is not wedged busy with a task it never ran.
		if clearErr := r.clearNodeCurrentTask(ctx, nodeName, taskKey(&task)); clearErr != nil {
			log.Error(clearErr, "failed to release reservation after schedule error",
				"node", nodeName, "task", task.Name)
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// taskOutcomeLabels extracts the bounded label values the operator emits for a
// terminal Foreman task (#1491): the Agent name (from spec.agentRef; "" when
// unset), the task kind, the verdict, and the machine outcome carried at
// result.extra.outcome ("" when the run has none). It also returns the
// executor's elapsedSec and the agent-loop turn count from result.extra.
//
// The task NAME is deliberately never a label: it is unbounded and would break
// cardinality. A missing or unparseable result envelope yields zero elapsedSec
// and zero turns (RecordTaskOutcome skips observing zeros) and an empty
// outcome — never a fabricated value.
func taskOutcomeLabels(task *foremanv1alpha1.AgenticTask) (agent, kind, verdict, outcome string, elapsedSec float64, turns int) {
	agent = ""
	if task.Spec.AgentRef != nil {
		agent = task.Spec.AgentRef.Name
	}
	kind = string(task.Spec.Kind)
	verdict = string(task.Status.Verdict)
	if task.Status.Result != nil && len(task.Status.Result.Raw) > 0 {
		var env struct {
			ElapsedSec float64 `json:"elapsedSec"`
			Extra      struct {
				Outcome   string `json:"outcome"`
				TurnCount int    `json:"turnCount"`
			} `json:"extra"`
		}
		if err := json.Unmarshal(task.Status.Result.Raw, &env); err == nil {
			outcome = env.Extra.Outcome
			elapsedSec = env.ElapsedSec
			turns = env.Extra.TurnCount
		}
	}
	return agent, kind, verdict, outcome, elapsedSec, turns
}

// effectiveRequiredCapability returns the capability the scheduler should
// match against. When spec.agentRef is set the Agent's capability wins;
// otherwise the task's own. NotFound on AgentRef is propagated so the
// caller can fail the task with a clear reason.
//
// The trailing bool reports whether the referenced Agent runs in Job mode
// (execution.mode: Job). In Job mode the model is remote and the agent
// loop runs in an ephemeral Job, so the claiming node only needs the
// role/nodeSelector: the scheduler relaxes the model-binding capability
// gates. The no-AgentRef path is never Job mode (false). See #620.
func (r *AgenticTaskReconciler) effectiveRequiredCapability(ctx context.Context, task *foremanv1alpha1.AgenticTask) (foremanv1alpha1.RequiredCapability, string, bool, error) {
	if task.Spec.AgentRef == nil || task.Spec.AgentRef.Name == "" {
		return task.Spec.RequiredCapability, "", false, nil
	}
	var agent foremanv1alpha1.Agent
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.AgentRef.Name}
	if err := r.Get(ctx, key, &agent); err != nil {
		return foremanv1alpha1.RequiredCapability{}, "", false, err
	}
	jobMode := agent.Spec.Execution != nil && agent.Spec.Execution.Mode == foremanv1alpha1.ExecutionModeJob
	return agent.Spec.RequiredCapability, agentModelIdentity(&agent), jobMode, nil
}

// agentAtConcurrencyLimit reports whether the task's referenced Agent has
// reached its spec.maxConcurrentTasks bound. When the Agent sets no bound
// (nil) the answer is always false (unbounded, today's behavior). Otherwise
// it lists the Agent's in-flight tasks in the same namespace — those whose
// phase actually holds a slot (Scheduled / Running / Verifying), not merely
// those that have not finished — and returns true when their count is
// already at or above the bound, so the caller leaves this task Pending for
// the next pass.
func (r *AgenticTaskReconciler) agentAtConcurrencyLimit(ctx context.Context, task *foremanv1alpha1.AgenticTask) (bool, error) {
	if task.Spec.AgentRef == nil || task.Spec.AgentRef.Name == "" {
		return false, nil
	}
	var agent foremanv1alpha1.Agent
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.AgentRef.Name}
	if err := r.Get(ctx, key, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			// The Agent is missing; effectiveRequiredCapability fails the task
			// with AgentNotFound before this point is reached, so treat as
			// unbounded here.
			return false, nil
		}
		return false, err
	}
	if agent.Spec.MaxConcurrentTasks == nil {
		return false, nil
	}
	var tasks foremanv1alpha1.AgenticTaskList
	if err := r.List(ctx, &tasks, client.InNamespace(task.Namespace)); err != nil {
		return false, err
	}
	inFlight := 0
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.AgentRef == nil || t.Spec.AgentRef.Name != agent.Name {
			continue
		}
		// The task being reconciled is still Pending and not yet in flight;
		// it must not count against the bound it is trying to claim into.
		if t.Name == task.Name {
			continue
		}
		if !holdsInFlightSlot(t.Status.Phase) {
			continue
		}
		inFlight++
	}
	return inFlight >= int(*agent.Spec.MaxConcurrentTasks), nil
}

// holdsInFlightSlot reports whether a task phase actually occupies an
// in-flight slot on its Agent. Only Scheduled, Running and Verifying hold a
// slot: Pending does NOT, by definition — it is the state a task sits in
// while waiting for this very dispatch decision. Counting Pending against
// the bound would deadlock a queued batch: with N Pending tasks and a bound
// of N or fewer, every task would see the others as "in flight", decline,
// and nothing would ever start. Counting "what holds a slot" (rather than
// "what has not finished") matches the field's godoc: the bound caps how
// many tasks may be *in flight* at once.
func holdsInFlightSlot(phase foremanv1alpha1.AgenticTaskPhase) bool {
	switch phase {
	case foremanv1alpha1.AgenticTaskPhaseScheduled,
		foremanv1alpha1.AgenticTaskPhaseRunning,
		foremanv1alpha1.AgenticTaskPhaseVerifying:
		return true
	default:
		return false
	}
}

// agentModelIdentity returns the model name used to test installedModels
// membership for RequiresModelInstalled scheduling. Prefers the explicit
// spec.model; falls back to the InferenceService reference name (which,
// in single-model fleets, matches the advertised installedModels entry).
func agentModelIdentity(agent *foremanv1alpha1.Agent) string {
	if agent.Spec.Model != "" {
		return agent.Spec.Model
	}
	return agent.Spec.InferenceServiceRef.Name
}

// setInitialPending writes phase=Pending the first time we see the task.
// The status patch triggers a fresh reconcile via the controller's
// For(AgenticTask) watch, so we do not need an explicit requeue (avoiding
// the deprecated Result.Requeue boolean).
func (r *AgenticTaskReconciler) setInitialPending(ctx context.Context, task *foremanv1alpha1.AgenticTask) (ctrl.Result, error) {
	patch := client.MergeFrom(task.DeepCopy())
	task.Status.Phase = foremanv1alpha1.AgenticTaskPhasePending
	if err := r.Status().Patch(ctx, task, patch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// cascadeFailIfDepFailed returns a non-empty message if any dependency
// is terminal-without-success; the caller fails the task with that
// message.
//
// "Terminal without success" means either:
//   - Phase=Failed (executor errored out), OR
//   - Phase=Succeeded but verdict in {INCOMPLETE, NO-GO, GATE-FAIL,
//     GATE-ERROR}. The dep is done, but it did not produce a usable
//     artifact. A downstream verify task should not run against a
//     branch the coder never pushed; a downstream review task should
//     not run against a coder verdict that already declined.
//
// Previously this gated on Phase=Failed only, which leaked INCOMPLETE
// coder tasks through to their downstream verifiers and made the
// downstream task fail GATE-FAIL on a clone-of-nonexistent-branch
// (the wrong reason). Fixes defilantech/LLMKube#541.
func (r *AgenticTaskReconciler) cascadeFailIfDepFailed(ctx context.Context, task *foremanv1alpha1.AgenticTask) (string, error) {
	for _, depName := range task.Spec.DependsOn {
		var dep foremanv1alpha1.AgenticTask
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: depName}, &dep); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", err
		}
		if dep.Status.Phase == foremanv1alpha1.AgenticTaskPhaseFailed {
			return fmt.Sprintf("dependency %q failed; cascade-failing", depName), nil
		}
		// Phase=Succeeded but verdict not on-target = terminal without
		// usable output. Cascade-fail so dependents don't run against
		// a nonexistent artifact.
		if dep.Status.Phase == foremanv1alpha1.AgenticTaskPhaseSucceeded && !dep.SucceededOnTarget() {
			return fmt.Sprintf("dependency %q ended with verdict=%s (not on-target); cascade-failing",
				depName, dep.Status.Verdict), nil
		}
	}
	return "", nil
}

// allDepsSucceeded returns true only when every dependency exists in
// the same namespace AND is on-target (Phase=Succeeded AND verdict in
// {GO, GATE-PASS}).
//
// A dependency that has not yet appeared is legal to wait for (a
// dependent can be created before its dependency in the ordering race the
// cascade-fail loop's IsNotFound branch tolerates). This records a
// DepWaitStarted condition so the stall is diagnosable; the budget check
// that actually cascade-fails an expired wait lives in cascadeFailIfMissing.
// Without #1687 a missing dep left the task Pending forever with no
// condition, event, or timeout.
//
// Previously gated on Phase=Succeeded alone, allowing INCOMPLETE /
// GATE-FAIL deps to unblock dependents. Fixes defilantech/LLMKube#541.
func (r *AgenticTaskReconciler) allDepsSucceeded(ctx context.Context, task *foremanv1alpha1.AgenticTask) (bool, error) {
	for _, depName := range task.Spec.DependsOn {
		var dep foremanv1alpha1.AgenticTask
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: depName}, &dep); err != nil {
			if apierrors.IsNotFound(err) {
				// Surface the wait; a budget-expired dep is cascade-failed
				// by cascadeFailIfMissing, so this keeps waiting for the
				// dep to appear within budget. See #1687.
				if err := r.recordMissingDepCondition(ctx, task, depName); err != nil {
					return false, err
				}
				return false, nil
			}
			return false, err
		}
		if !dep.SucceededOnTarget() {
			return false, nil
		}
	}
	return true, nil
}

// cascadeFailIfMissing returns a non-empty message if any dependency is
// still absent (not found) and the task's wait for it has outlasted the
// TimeoutSeconds budget; otherwise it returns "" so the caller keeps
// waiting. The missing-dep cascade-fail is checked before
// cascadeFailIfDepFailed so the Failed condition carries the distinct
// MissingDependency reason (#1687).
func (r *AgenticTaskReconciler) cascadeFailIfMissing(ctx context.Context, task *foremanv1alpha1.AgenticTask) (string, error) {
	for _, depName := range task.Spec.DependsOn {
		var dep foremanv1alpha1.AgenticTask
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: depName}, &dep); err != nil {
			if apierrors.IsNotFound(err) {
				if depWaitExpired(task) {
					return fmt.Sprintf("dependency %q never appeared within TimeoutSeconds=%d; cascade-failing",
						depName, task.Spec.TimeoutSeconds), nil
				}
				continue
			}
			return "", err
		}
	}
	return "", nil
}

// recordMissingDepCondition writes a DepWaitStarted condition on the task
// naming the absent dependency, so the wait is diagnosable via
// kubectl describe. It patches only when the condition is not already
// recorded with the same shape (Type/Status/Reason), so a dep that stays
// absent does not wedge the reconciler in a per-reconcile status-patch
// loop. The caller keeps waiting (returns false, nil) for the dep to
// appear. See #1687.
func (r *AgenticTaskReconciler) recordMissingDepCondition(ctx context.Context, task *foremanv1alpha1.AgenticTask, depName string) error {
	depWait := metav1.Condition{
		Type:               "DepWaitStarted",
		Status:             metav1.ConditionTrue,
		Reason:             "DependencyAbsent",
		Message:            fmt.Sprintf("dependency %q not found; waiting for it to be created", depName),
		LastTransitionTime: metav1.Now(),
	}
	if hasCondition(task.Status.Conditions, depWait) {
		return nil
	}
	// Capture the merge base BEFORE mutating, so the computed diff actually
	// carries the newly added condition to the API server. Capturing after
	// setCondition would make the base already contain it, so the merge
	// patch would see no diff and the condition would never persist.
	patch := client.MergeFrom(task.DeepCopy())
	setCondition(&task.Status.Conditions, depWait)
	return r.Status().Patch(ctx, task, patch)
}

// depWaitExpired reports whether the task's wait for an absent dependency
// has outlasted its TimeoutSeconds budget, in which case the caller should
// cascade-fail the task with a MissingDependency reason.
//
// The budget is measured from the DepWaitStarted condition's
// LastTransitionTime -- the moment the reconciler FIRST observed the
// dependency absent -- and NOT from the task's creation. Two reasons, and
// the first is the load-bearing one:
//
//   - Semantics. The budget is for the wait, not for the task's lifetime. A
//     task created well before its dependency is dispatched would already be
//     past a creation-relative budget on its first reconcile and cascade-fail
//     instantly, which is the create-ordering race this whole path exists to
//     tolerate.
//   - Testability. metadata.creationTimestamp is assigned by the API server
//     and is immutable: an Update that backdates it returns success and
//     silently stores the original value. A creation-relative budget is
//     therefore unreachable from an envtest, so no test can prove the
//     cascade-fail ever fires. status.conditions are ordinary status and can
//     be written, so the condition clock is reachable.
//
// No condition means the wait has not been recorded yet, so nothing has
// elapsed. An unset or zero TimeoutSeconds falls back to the package-level
// defaultDepWaitBudget so the wait is bounded even on paths (such as the
// planner) that never stamp the field; an explicitly set positive
// TimeoutSeconds always wins.
func depWaitExpired(task *foremanv1alpha1.AgenticTask) bool {
	budget := time.Duration(task.Spec.TimeoutSeconds) * time.Second
	if budget <= 0 {
		budget = defaultDepWaitBudget
	}
	started := apimeta.FindStatusCondition(task.Status.Conditions, "DepWaitStarted")
	if started == nil || started.LastTransitionTime.IsZero() {
		return false
	}
	return time.Since(started.LastTransitionTime.Time) >= budget
}

// reserveFirstFitNode picks the alphabetically-first Ready FleetNode whose
// advertised capability satisfies the effective RequiredCapability and that is
// not already running another task, and atomically reserves it by stamping the
// node's Status.CurrentTask. It returns the reserved node's name, or "" when no
// eligible node is currently free.
//
// Reservation is what makes one-task-per-node spread work across the fleet. The
// node scan reads FleetNodes from a cache that lags writes, so two back-to-back
// scheduling reconciles can both observe the same node as free. The reservation
// is an optimistic-lock status patch, so at most one wins; the loser (Conflict,
// or a node already reserved for a live task) falls through to the next
// candidate. Without this, every task funnels onto one node. See #977.
//
// In Job mode the task's work runs in an ephemeral Job pod, not on the claiming
// node, so the node must NOT be reserved: stamping CurrentTask would hold a
// one-task-per-node slot for the Job's whole lifetime while the node itself
// does nothing but wait, starving in-process tasks that could run there. A
// Job-mode task is still assigned to a node (so the Job is created there), but
// the node's CurrentTask is left untouched and stays free for in-process work.
// See #1496.
func (r *AgenticTaskReconciler) reserveFirstFitNode(ctx context.Context, task *foremanv1alpha1.AgenticTask, required foremanv1alpha1.RequiredCapability, requiredModel string, jobMode bool) (string, error) {
	var nodes foremanv1alpha1.FleetNodeList
	if err := r.List(ctx, &nodes); err != nil {
		return "", err
	}
	sort.Slice(nodes.Items, func(i, j int) bool {
		return nodes.Items[i].Name < nodes.Items[j].Name
	})
	now := time.Now()
	key := taskKey(task)
	// eligible holds the sorted indices of nodes that are schedulable and
	// satisfy the capability. A first pass collects them so the jobMode
	// branch can index into the eligible list directly (the index of the
	// eligible node being considered must be known before it is returned).
	var eligible []int
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if !nodeSchedulable(n, now) {
			continue
		}
		if !capabilitySatisfies(required, requiredModel, n, jobMode) {
			continue
		}
		eligible = append(eligible, i)
	}
	if jobMode {
		// Job-mode work runs in an ephemeral Job pod, not on this node, so
		// the node must not be claimed: leave its CurrentTask untouched so
		// it stays free for in-process tasks. See #1496.
		//
		// Reserving would undo #1496, but the reservation is what used to
		// make the scan advance; without it every Job-mode task lands on the
		// alphabetically first node, so a fleet of N nodes can supervise only
		// one Job at a time. See #1634.
		//
		// Pick the eligible node currently supervising the fewest live
		// Job-mode tasks. An earlier revision rotated on a scalar count of
		// live tasks (count % len(eligible)); that spreads a burst but
		// collapses to index 0 -- first-fit -- whenever nothing else is in
		// flight, which is the common serial case.
		if len(eligible) == 0 {
			return "", nil
		}
		return nodes.Items[r.leastLoadedJobModeNode(ctx, nodes.Items, eligible, task)].Name, nil
	}
	for _, i := range eligible {
		n := &nodes.Items[i]
		reserved, err := r.reserveNode(ctx, n, key)
		if err != nil {
			return "", err
		}
		if reserved {
			return n.Name, nil
		}
		// Node was busy with a live task, or another reconcile reserved it
		// first; try the next candidate.
	}
	return "", nil
}

// leastLoadedJobModeNode returns the index into nodes of the eligible node
// supervising the fewest live Job-mode tasks, so successive assignments spread
// across the fleet instead of stacking on the alphabetically first node
// (see #1634). eligible is the eligible-node index slice built by
// reserveFirstFitNode (sorted by name); the caller guarantees it is non-empty.
//
// Ties break toward the earlier eligible node, so with an idle fleet this is
// still deterministic. Unlike a scalar rotation it cannot collapse to index 0
// when nothing is in flight, and it distinguishes a node already supervising
// three Jobs from an idle one.
func (r *AgenticTaskReconciler) leastLoadedJobModeNode(
	ctx context.Context, nodes []foremanv1alpha1.FleetNode, eligible []int, task *foremanv1alpha1.AgenticTask,
) int {
	load := r.jobModeLoadByNode(ctx, task)
	best := eligible[0]
	bestLoad := load[nodes[best].Name]
	for _, i := range eligible[1:] {
		if l := load[nodes[i].Name]; l < bestLoad {
			best, bestLoad = i, l
		}
	}
	return best
}

// jobModeLoadByNode tallies the in-flight Job-mode AgenticTasks in the task's
// namespace per assigned node: those in a Scheduled (or later) phase that carry
// an AssignedNode, regardless of whether the node is reserved. Job-mode tasks
// are never reserved (#1496), so they cannot be counted via CurrentTask; the
// assigned-node stamp is the only durable record of which Jobs a node currently
// supervises, and it is a real per-node load signal.
//
// Every agent's Job-mode work counts toward a node's load, not just the
// scheduling task's own agent: two agents dispatching concurrently are
// competing for the same node capacity, and filtering by AgentRef would let
// each of them independently pick the same "idle" node.
//
// The task being scheduled is still Pending, so it does not count against
// itself.
func (r *AgenticTaskReconciler) jobModeLoadByNode(ctx context.Context, task *foremanv1alpha1.AgenticTask) map[string]int {
	load := map[string]int{}
	var tasks foremanv1alpha1.AgenticTaskList
	// A failing list must not wedge scheduling; an empty tally falls back to
	// the first eligible node so the task still dispatches rather than
	// stalling.
	if err := r.List(ctx, &tasks, client.InNamespace(task.Namespace)); err != nil {
		return load
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if !holdsInFlightSlot(t.Status.Phase) {
			continue
		}
		if t.Status.AssignedNode == "" {
			continue
		}
		load[t.Status.AssignedNode]++
	}
	return load
}

// reserveNode stamps node.Status.CurrentTask with taskKey via an optimistic-
// lock status patch, claiming the node for a single task. It returns false
// (try the next node) when the node is already running another live task or
// when a concurrent writer won the node first (Conflict).
//
// A CurrentTask that points at a task which no longer exists or has reached a
// terminal phase is stale: the node is treated as free and re-reserved. That
// self-heals a reservation leaked by a task deleted mid-flight, so a missed
// clear cannot wedge a node busy forever.
func (r *AgenticTaskReconciler) reserveNode(ctx context.Context, node *foremanv1alpha1.FleetNode, taskKey string) (bool, error) {
	if cur := node.Status.CurrentTask; cur != "" && cur != taskKey {
		live, err := taskIsLive(ctx, r.Client, cur, node.Name)
		if err != nil {
			return false, err
		}
		if live {
			return false, nil // genuinely busy
		}
	}
	patch := client.MergeFromWithOptions(node.DeepCopy(), client.MergeFromWithOptimisticLock{})
	node.Status.CurrentTask = taskKey
	if err := r.Status().Patch(ctx, node, patch); err != nil {
		// Lost the race: another reconcile reserved it first (Conflict), or the
		// node was deleted between the List and here (NotFound — e.g. the
		// Draining-node reaper, #980/#979). Either way, fall through to the next
		// candidate rather than erroring the whole reconcile.
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// taskIsLive reports whether the namespaced-name key still occupies nodeName:
// the task exists, has not reached a terminal phase, and has not been assigned
// to a different node. A missing, terminal, or reassigned task is not live, so
// a node whose CurrentTask points at it may be reclaimed. An empty AssignedNode
// counts as live — that is the legitimate window between reserveNode and
// scheduleToNode, which must not be stolen. Shared with the FleetNode
// reconciler's reservation hygiene, which runs the same predicate against a
// task deleted outright (#1791).
func taskIsLive(ctx context.Context, c client.Client, key, nodeName string) (bool, error) {
	ns, name, ok := splitNamespacedName(key)
	if !ok {
		return false, nil // unparseable key: treat as not live so the node frees
	}
	var t foremanv1alpha1.AgenticTask
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &t); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if t.Status.Phase == foremanv1alpha1.AgenticTaskPhaseSucceeded ||
		t.Status.Phase == foremanv1alpha1.AgenticTaskPhaseFailed {
		return false, nil
	}
	// A live task that has been scheduled onto a different node no longer holds
	// this one; the reservation is stale (e.g. a clear that failed log-and-
	// continue while the task rescheduled elsewhere). Reclaim it.
	if an := t.Status.AssignedNode; an != "" && an != nodeName {
		return false, nil
	}
	return true, nil
}

// clearNodeCurrentTask releases the named FleetNode's reservation, but only if
// it still points at taskKey, so a node already re-reserved for a different
// task is left untouched. A missing node (or empty name) is a no-op.
func (r *AgenticTaskReconciler) clearNodeCurrentTask(ctx context.Context, nodeName, taskKey string) error {
	if nodeName == "" {
		return nil
	}
	var node foremanv1alpha1.FleetNode
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		return client.IgnoreNotFound(err)
	}
	if node.Status.CurrentTask != taskKey {
		return nil
	}
	// Optimistic lock for symmetry with reserveNode: between the Get above and
	// this patch a concurrent reserve for a different task could land, and an
	// unlocked merge would stomp that fresh reservation (the double-book class
	// this fix exists to prevent). On Conflict the caller logs-and-continues and
	// the stale-reclaim path in reserveNode self-heals.
	patch := client.MergeFromWithOptions(node.DeepCopy(), client.MergeFromWithOptimisticLock{})
	node.Status.CurrentTask = ""
	return r.Status().Patch(ctx, &node, patch)
}

// taskKey is the "namespace/name" string stored in FleetNode.Status.CurrentTask.
func taskKey(task *foremanv1alpha1.AgenticTask) string {
	return types.NamespacedName{Namespace: task.Namespace, Name: task.Name}.String()
}

// splitNamespacedName parses the "namespace/name" form written by taskKey.
func splitNamespacedName(key string) (namespace, name string, ok bool) {
	i := strings.IndexByte(key, '/')
	if i < 0 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}

// nodeSchedulable reports whether the scheduler may dispatch to a node. It
// must read Phase=Ready AND have a fresh heartbeat: the FleetNodeReconciler
// flips a dead node to NotReady, but that is level-triggered and may lag a
// heartbeat. Re-checking staleness here (defense-in-depth) prevents
// dispatching a task into a node whose agent has gone dark but whose phase
// has not yet been reconciled. See defilantech/LLMKube#627.
func nodeSchedulable(n *foremanv1alpha1.FleetNode, now time.Time) bool {
	if n.Status.Phase != foremanv1alpha1.FleetNodePhaseReady {
		return false
	}
	return !n.HeartbeatStale(now)
}

// capabilitySatisfies returns true when the node's advertised capability
// meets every requirement the task declares. Unset requirements are
// unconstrained; an "any" accelerator matches everything.
//
// When jobMode is true the agent loop runs in an ephemeral Job and the
// model is a remote (HTTP) dependency, so the claiming node does not host
// the model: the accelerator, RequiresModelInstalled, minRAMGB, and
// minContextTokens gates (all of which bind a node to a locally-resident
// model) are skipped. nodeSelector and roles still apply, since they
// constrain where the Job's claiming node may live. See #620.
func capabilitySatisfies(req foremanv1alpha1.RequiredCapability, requiredModel string, n *foremanv1alpha1.FleetNode, jobMode bool) bool {
	cap := n.Status.Capability

	if !jobMode {
		if req.Accelerator != "" && req.Accelerator != foremanv1alpha1.AgenticTaskAccelerator("any") {
			if string(cap.Accelerator) != string(req.Accelerator) {
				return false
			}
		}
		if req.RequiresModelInstalled {
			// Warm-driver path: the Agent's model must already be resident on
			// the node, and the minRAMGB gate is bypassed (the loaded model
			// has already paid the RAM cost; the loop adds ~0). An empty
			// requiredModel is a misconfiguration we cannot confirm, so it
			// fails the match rather than silently bypassing the gate.
			// See defilantech/LLMKube#579.
			if requiredModel == "" || !slices.Contains(cap.InstalledModels, requiredModel) {
				return false
			}
		} else if req.MinRAMGB > 0 && req.MinRAMGB > cap.AvailableRAMGB {
			return false
		}
		if req.MinContextTokens > 0 && req.MinContextTokens > cap.MaxContextTokens {
			return false
		}
	}
	for k, v := range req.NodeSelector {
		if n.Labels[k] != v {
			return false
		}
	}
	if len(req.Roles) > 0 {
		have := make(map[string]struct{}, len(n.Spec.Roles))
		for _, r := range n.Spec.Roles {
			have[r] = struct{}{}
		}
		for _, want := range req.Roles {
			if _, ok := have[want]; !ok {
				return false
			}
		}
	}
	return true
}

// scheduleToNode patches the task to phase=Scheduled with the chosen
// FleetNode set on status.assignedNode. The FleetAgent on that node
// picks it up via its watcher.
func (r *AgenticTaskReconciler) scheduleToNode(ctx context.Context, task *foremanv1alpha1.AgenticTask, nodeName string) error {
	patch := client.MergeFrom(task.DeepCopy())
	now := metav1.Now()
	task.Status.Phase = foremanv1alpha1.AgenticTaskPhaseScheduled
	task.Status.AssignedNode = nodeName
	setCondition(&task.Status.Conditions, metav1.Condition{
		Type:               "Scheduled",
		Status:             metav1.ConditionTrue,
		Reason:             "FleetNodeAssigned",
		Message:            fmt.Sprintf("scheduled to FleetNode %q", nodeName),
		LastTransitionTime: now,
	})
	return r.Status().Patch(ctx, task, patch)
}

// failTask cascade-fails a Pending task before it reaches an agent.
func (r *AgenticTaskReconciler) failTask(ctx context.Context, task *foremanv1alpha1.AgenticTask, reason, message string) (ctrl.Result, error) {
	patch := client.MergeFrom(task.DeepCopy())
	now := metav1.Now()
	task.Status.Phase = foremanv1alpha1.AgenticTaskPhaseFailed
	task.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictIncomplete
	task.Status.FinishedAt = &now
	setCondition(&task.Status.Conditions, metav1.Condition{
		Type:               "Failed",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	})
	return ctrl.Result{}, r.Status().Patch(ctx, task, patch)
}

// skipTask transitions a Pending task to Phase=Succeeded + Verdict=Skipped
// because its dependency ended ALREADY-RESOLVED or was itself Skipped
// (#970, transitive #1688), or ended with a cross-stage contradiction
// (#1686). The Workload rollup recognizes Skipped as a terminal-non-failure
// shape that is excluded from every bucket; the dependent's execution never
// happened, so no FailureReason is set (the cause is upstream, recorded in
// the condition message). The Failed condition is NOT set — this is the
// explicit non-failure path.
//
// depState is the dependency's own terminal state string ("ALREADY-RESOLVED"
// or "Skipped"), threaded through so the condition message names the actual
// reason rather than always saying ALREADY-RESOLVED for a transitive skip.
// When contradicted is true the dependency ended with a cross-stage
// contradiction: skipTask stamps the distinct UpstreamContradicted reason and
// names both the dependency and the contradiction text, so this stop signal
// stays alertable separately from the benign UpstreamAlreadyResolved reason.
func (r *AgenticTaskReconciler) skipTask(ctx context.Context, task *foremanv1alpha1.AgenticTask, depName, depState string, contradicted bool) (ctrl.Result, error) {
	patch := client.MergeFrom(task.DeepCopy())
	now := metav1.Now()
	task.Status.Phase = foremanv1alpha1.AgenticTaskPhaseSucceeded
	task.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictSkipped
	task.Status.FinishedAt = &now
	reason := "UpstreamAlreadyResolved"
	var message string
	if contradicted {
		reason = "UpstreamContradicted"
		message = fmt.Sprintf("dependency %q ended with a contradiction; dependent skipped (%s)", depName, depState)
	} else {
		message = fmt.Sprintf("dependency %q ended %s; dependent skipped", depName, depState)
	}
	setCondition(&task.Status.Conditions, metav1.Condition{
		Type:               "Skipped",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	})
	return ctrl.Result{}, r.Status().Patch(ctx, task, patch)
}

// cascadeSkipIfDepResolvedOrSkipped returns the first dependency that ended
// ALREADY-RESOLVED (#970) OR was itself Skipped, or "" if none. The caller
// transitions the dependent to Skipped; see skipTask. Runs before
// cascadeFailIfDepFailed in Reconcile so a resolved/skipped dep never trips
// the "terminal without success" branch.
//
// Transitivity: a dependency that was itself cascade-skipped (because ITS
// dependency ended ALREADY-RESOLVED) would otherwise fall through to
// cascadeFailIfDepFailed, which treats verdict=Skipped as "terminal without
// success" and cascade-fails the dependent. Checking isSkippedTask here makes
// the skip walk the whole dependency chain rather than stopping after one
// hop. Fixes defilantech/LLMKube#1688.
func (r *AgenticTaskReconciler) cascadeSkipIfDepResolvedOrSkipped(ctx context.Context, task *foremanv1alpha1.AgenticTask) (string, string, error) {
	for _, depName := range task.Spec.DependsOn {
		var dep foremanv1alpha1.AgenticTask
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: depName}, &dep); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", "", err
		}
		if isAlreadyResolvedCoder(&dep) {
			return depName, "ALREADY-RESOLVED", nil
		}
		if isSkippedTask(&dep) {
			return depName, "Skipped", nil
		}
	}
	return "", "", nil
}

// cascadeSkipIfDepContradicted returns the first dependency that ended with a
// cross-stage contradiction (a non-empty extra.crossStageContradictions in its
// terminal result envelope, #1686), or "" if none. The second return value is
// the contradiction text (the first contradiction string) the caller threads
// into the skip condition message. The caller transitions the dependent to
// Skipped; see skipTask. Runs AFTER cascadeSkipIfDepResolvedOrSkipped in
// Reconcile so a dependency that is both ALREADY-RESOLVED and contradicted
// stays skipped as already-resolved (the benign terminal non-failure wins over
// the stop signal), and runs BEFORE cascadeFailIfDepFailed so the contradiction
// skips the dependent rather than cascade-failing it (which would pin the
// Workload to Failed).
//
// Uses hasCrossStageContradiction / coderCrossStageContradictions from the
// predecessor issue (#1685) rather than re-deriving the signal: the detector
// writes the contradiction list in the coder / gate / reviewer wiring, and the
// controller only reads it. Blocking here is dependents-only: the contradicting
// task's own verdict and phase are left untouched (demoting it was considered
// and rejected — it would turn a records-only rail into a verdict-changing one).
func (r *AgenticTaskReconciler) cascadeSkipIfDepContradicted(ctx context.Context, task *foremanv1alpha1.AgenticTask) (string, string, error) {
	for _, depName := range task.Spec.DependsOn {
		var dep foremanv1alpha1.AgenticTask
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: depName}, &dep); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", "", err
		}
		if !hasCrossStageContradiction(&dep) {
			continue
		}
		cs := coderCrossStageContradictions(&dep)
		contradiction := ""
		if len(cs) > 0 {
			contradiction = cs[0]
		}
		return depName, contradiction, nil
	}
	return "", "", nil
}

// checkClaimExpiry inspects the FleetNode named by task.status.assignedNode.
// If the node is absent or its heartbeat is stale the task is either
// released back to Pending (3-strike ladder: < claimExpiryLimit prior
// expiries) or terminal-failed (>= claimExpiryLimit prior expiries).
// If the node is fresh the task is left untouched and a requeue is scheduled
// at FleetNodeHeartbeatTimeout/2 so staleness is caught promptly without
// relying solely on events.
//
// Counter ordering: the metadata annotation (counter) is updated BEFORE the
// status patch so that a crash between the two errs toward counting more
// expiries rather than fewer, bounding poison loops conservatively.
func (r *AgenticTaskReconciler) checkClaimExpiry(ctx context.Context, task *foremanv1alpha1.AgenticTask) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var node foremanv1alpha1.FleetNode
	nodeNotFound := false
	lastHeartbeatMsg := ""

	if err := r.Get(ctx, types.NamespacedName{Name: task.Status.AssignedNode}, &node); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get FleetNode %s: %w", task.Status.AssignedNode, err)
		}
		nodeNotFound = true
		lastHeartbeatMsg = "FleetNode not found"
	}

	// Node is present and heartbeat is fresh: nothing to do yet.
	if !nodeNotFound && !node.HeartbeatStale(time.Now()) {
		return ctrl.Result{RequeueAfter: foremanv1alpha1.FleetNodeHeartbeatTimeout / 2}, nil
	}

	if !nodeNotFound {
		// Build the last-heartbeat context for the condition message.
		if node.Status.LastHeartbeatTime != nil {
			lastHeartbeatMsg = fmt.Sprintf("last heartbeat %s", node.Status.LastHeartbeatTime.Format(time.RFC3339))
		} else {
			lastHeartbeatMsg = "no heartbeat recorded"
		}
	}

	// Read the prior expiry count from the annotation.
	prior := 0
	if raw, ok := task.Annotations[claimExpiriesAnnotation]; ok {
		if n, err := strconv.Atoi(raw); err == nil {
			prior = n
		}
	}

	nodeName := task.Status.AssignedNode

	if prior >= claimExpiryLimit {
		// 3-strike limit reached: terminal-fail.
		log.Info("claim expiry limit reached; terminal-failing task",
			"task", task.Name, "node", nodeName, "priorExpiries", prior)
		return r.terminalFailExpired(ctx, task, nodeName, prior)
	}

	// Release: increment counter first (crash-safe ordering), then release.
	log.Info("claim expired; releasing task to Pending",
		"task", task.Name, "node", nodeName, "priorExpiries", prior, "heartbeat", lastHeartbeatMsg)
	if err := r.incrementExpiryCounter(ctx, task, prior); err != nil {
		return ctrl.Result{}, err
	}
	return r.releaseExpiredClaim(ctx, task, nodeName, lastHeartbeatMsg)
}

// incrementExpiryCounter writes max(freshValue, snapshotValue)+1 into the
// claim-expiries annotation, where freshValue is read from the live object.
// Using the live value closes an informer-lag window: if a prior expiry
// landed between the reconcile snapshot and this write, the snapshot's
// counter would be stale and we would double-count the expiry on the next
// reconcile (or under-count after a crash). Taking the max ensures we
// never regress the counter regardless of which view is ahead.
//
// This is a metadata (non-status) Update, distinct from the status patch
// that follows. It must happen first so a crash between the two errs toward
// counting more expiries.
func (r *AgenticTaskReconciler) incrementExpiryCounter(ctx context.Context, task *foremanv1alpha1.AgenticTask, snapshotPrior int) error {
	// Re-fetch to get the current resourceVersion for the metadata patch.
	var current foremanv1alpha1.AgenticTask
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, &current); err != nil {
		return fmt.Errorf("re-fetch for expiry counter: %w", err)
	}
	// Read the freshly-fetched counter; fall back to 0 if absent/invalid.
	freshPrior := 0
	if raw, ok := current.Annotations[claimExpiriesAnnotation]; ok {
		if n, err := strconv.Atoi(raw); err == nil {
			freshPrior = n
		}
	}
	// Advance from whichever view is higher to avoid regressing the counter.
	base := freshPrior
	if snapshotPrior > base {
		base = snapshotPrior
	}
	patch := client.MergeFrom(current.DeepCopy())
	if current.Annotations == nil {
		current.Annotations = map[string]string{}
	}
	current.Annotations[claimExpiriesAnnotation] = strconv.Itoa(base + 1)
	return r.Patch(ctx, &current, patch)
}

// releaseExpiredClaim resets the task to Pending, clears the claim fields,
// and records a ClaimExpired condition.
//
// Guard: after the re-fetch the function bails out without patching if the
// live object has already moved to a terminal phase (Succeeded/Failed) or if
// its AssignedNode no longer matches the node we judged stale. Either
// condition means a concurrent terminal patch landed in the window between
// checkClaimExpiry's staleness decision and this write; yanking the task back
// to Pending in that case would undo legitimate agent progress.
func (r *AgenticTaskReconciler) releaseExpiredClaim(ctx context.Context, task *foremanv1alpha1.AgenticTask, nodeName, heartbeatMsg string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var current foremanv1alpha1.AgenticTask
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, &current); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetch for claim release: %w", err)
	}
	if current.Status.Phase == foremanv1alpha1.AgenticTaskPhaseSucceeded ||
		current.Status.Phase == foremanv1alpha1.AgenticTaskPhaseFailed ||
		current.Status.AssignedNode != nodeName {
		log.Info("claim expiry superseded by concurrent state change; skipping release",
			"task", current.Name, "node", nodeName,
			"livePhase", current.Status.Phase, "liveAssignedNode", current.Status.AssignedNode)
		return ctrl.Result{}, nil
	}
	patch := client.MergeFrom(current.DeepCopy())
	now := metav1.Now()
	current.Status.Phase = foremanv1alpha1.AgenticTaskPhasePending
	current.Status.AssignedNode = ""
	current.Status.ClaimedAt = nil
	current.Status.StartedAt = nil
	setCondition(&current.Status.Conditions, metav1.Condition{
		Type:               "ClaimExpired",
		Status:             metav1.ConditionTrue,
		Reason:             "ClaimExpired",
		Message:            fmt.Sprintf("released from node %q: %s", nodeName, heartbeatMsg),
		LastTransitionTime: now,
	})
	if err := r.Status().Patch(ctx, &current, patch); err != nil {
		return ctrl.Result{}, err
	}
	// Free the node so the released task (or another) can be dispatched there.
	if err := r.clearNodeCurrentTask(ctx, nodeName, taskKey(&current)); err != nil {
		log.Error(err, "failed to release node reservation on claim expiry",
			"node", nodeName, "task", current.Name)
	}
	return ctrl.Result{}, nil
}

// terminalFailExpired marks a task Failed after it has exhausted the
// 3-strike expiry ladder.
//
// Guard: same as releaseExpiredClaim. If the live object is already terminal
// or has been reassigned away from the stale node, a concurrent patch already
// resolved the task; bail out without patching to avoid overwriting it.
func (r *AgenticTaskReconciler) terminalFailExpired(ctx context.Context, task *foremanv1alpha1.AgenticTask, nodeName string, priorExpiries int) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var current foremanv1alpha1.AgenticTask
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, &current); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetch for expiry terminal-fail: %w", err)
	}
	if current.Status.Phase == foremanv1alpha1.AgenticTaskPhaseSucceeded ||
		current.Status.Phase == foremanv1alpha1.AgenticTaskPhaseFailed ||
		current.Status.AssignedNode != nodeName {
		log.Info("claim expiry superseded by concurrent state change; skipping terminal-fail",
			"task", current.Name, "node", nodeName,
			"livePhase", current.Status.Phase, "liveAssignedNode", current.Status.AssignedNode)
		return ctrl.Result{}, nil
	}
	patch := client.MergeFrom(current.DeepCopy())
	now := metav1.Now()
	current.Status.Phase = foremanv1alpha1.AgenticTaskPhaseFailed
	current.Status.Verdict = foremanv1alpha1.AgenticTaskVerdictIncomplete
	current.Status.FailureReason = foremanv1alpha1.FailureInfrastructureError
	current.Status.FinishedAt = &now
	setCondition(&current.Status.Conditions, metav1.Condition{
		Type:   "Failed",
		Status: metav1.ConditionTrue,
		Reason: "ClaimExpiryLimitReached",
		Message: fmt.Sprintf("task on node %q expired %d time(s); terminal-failing to prevent poison loop",
			nodeName, priorExpiries+1),
		LastTransitionTime: now,
	})
	if err := r.Status().Patch(ctx, &current, patch); err != nil {
		return ctrl.Result{}, err
	}
	// The task is terminal-failed on this node; release the reservation.
	if err := r.clearNodeCurrentTask(ctx, nodeName, taskKey(&current)); err != nil {
		log.Error(err, "failed to release node reservation on expiry terminal-fail",
			"node", nodeName, "task", current.Name)
	}
	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler. We also watch FleetNode so a
// node going Ready (or freeing up CurrentTask) re-enqueues every Pending
// task immediately rather than waiting for the requeue-after timer.
func (r *AgenticTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&foremanv1alpha1.AgenticTask{}).
		Watches(&foremanv1alpha1.FleetNode{}, handler.EnqueueRequestsFromMapFunc(r.fleetNodeEnqueues)).
		Named("agentictask").
		Complete(r)
}

// fleetNodeEnqueues re-enqueues AgenticTasks when a FleetNode event arrives:
//   - Every Pending task (scheduling: a newly-Ready node may satisfy a waiting
//     task immediately rather than waiting for the requeue-after timer).
//   - Every in-flight task (Scheduled/Running/Verifying) whose AssignedNode
//     matches the changed node (claim expiry: a node going stale or deleted
//     must trigger the expiry check on its assigned task promptly rather than
//     waiting for the 45s backstop requeue).
//
// The workqueue dedupes so the worst-case cost is one reconcile per task per
// FleetNode event, which is acceptable at v0.1 task volumes.
func (r *AgenticTaskReconciler) fleetNodeEnqueues(ctx context.Context, obj client.Object) []ctrl.Request {
	var list foremanv1alpha1.AgenticTaskList
	if err := r.List(ctx, &list); err != nil {
		logf.FromContext(ctx).Error(err, "fleetnode-trigger list failed")
		return nil
	}
	changedNodeName := obj.GetName()
	requests := make([]ctrl.Request, 0, len(list.Items))
	seen := make(map[types.NamespacedName]struct{}, len(list.Items))
	for i := range list.Items {
		t := &list.Items[i]
		key := types.NamespacedName{Namespace: t.Namespace, Name: t.Name}
		switch t.Status.Phase {
		case foremanv1alpha1.AgenticTaskPhasePending:
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				requests = append(requests, ctrl.Request{NamespacedName: key})
			}
		case foremanv1alpha1.AgenticTaskPhaseScheduled,
			foremanv1alpha1.AgenticTaskPhaseRunning,
			foremanv1alpha1.AgenticTaskPhaseVerifying:
			if t.Status.AssignedNode == changedNodeName {
				if _, ok := seen[key]; !ok {
					seen[key] = struct{}{}
					requests = append(requests, ctrl.Request{NamespacedName: key})
				}
			}
		}
	}
	return requests
}

// transcriptDataKey is the ConfigMap data key the transcript is stored under.
// It is the only key read, so a producer that renames it silently drops every
// transcript in the fleet. That is why a ConfigMap which resolves but carries
// nothing under this key is counted below rather than quietly recorded as a
// run that produced no transcript.
const transcriptDataKey = "transcript.json"

// archiveTerminalTask writes one immutable bundle for a terminal task.
//
// Deliberately not gated on firstTerminal. WriteBundle skips a bundle that
// already exists, so calling this on every terminal reconcile costs one stat
// and gives a failed write a free retry: a failure leaves no directory behind.
// Gating on firstTerminal would turn a transient ENOSPC into permanent data
// loss for that task.
//
// Every failure path is non-fatal and counted. Archival must never change a
// verdict; the harness is the source of truth and this is a side effect.
func (r *AgenticTaskReconciler) archiveTerminalTask(
	ctx context.Context,
	task *foremanv1alpha1.AgenticTask,
	log logr.Logger,
) {
	if r.ArchiveDir == "" {
		return
	}
	rec := audit.BuildRecord(task, audit.ResolveAgent(ctx, r.Client, task, log))

	var transcript []byte
	if ref := task.Status.TranscriptRef; ref != "" {
		var cm corev1.ConfigMap
		key := client.ObjectKey{Namespace: task.Namespace, Name: ref}
		if err := r.Get(ctx, key, &cm); err != nil {
			// A deterministic run writes no transcript, and a transcript can be
			// collected before this runs. Neither is a reason to drop the record.
			log.V(1).Info("archive: transcript not readable; archiving record only",
				"transcriptRef", ref, "err", err.Error())
			llmkubemetrics.RecordArchiveFailure("transcript_read")
		} else if body := cm.Data[transcriptDataKey]; body == "" {
			// The ConfigMap resolved but holds nothing we can read. Without
			// this branch the bundle records hasTranscript:false, which is
			// indistinguishable from a deterministic run that legitimately
			// produced no transcript, so a producer-side key rename would
			// drop every transcript in the fleet with no signal at all.
			log.V(1).Info("archive: transcript configmap holds no transcript; archiving record only",
				"transcriptRef", ref, "key", transcriptDataKey)
			llmkubemetrics.RecordArchiveFailure("transcript_empty")
		} else {
			transcript = []byte(body)
		}
	}

	if err := archive.WriteBundle(r.ArchiveDir, rec, transcript); err != nil {
		log.Error(err, "archive: failed to write bundle (continuing)", "task", task.Name)
		llmkubemetrics.RecordArchiveFailure("write")
	}
}
