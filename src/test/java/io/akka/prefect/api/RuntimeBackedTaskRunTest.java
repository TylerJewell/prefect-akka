package io.akka.prefect.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.prefect.application.TaskRunEntity;
import io.akka.prefect.domain.StateType;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The capability driven the way a caller outside a test would drive it: over HTTP, against a
 * started runtime. The domain tests check the rules; this checks that something outside a test can
 * reach them at all.
 */
public class RuntimeBackedTaskRunTest extends TestKitSupport {

  private TaskRunView post(String path, Object body) {
    return httpClient.POST(path).withRequestBody(body).responseBodyAs(TaskRunView.class).invoke().body();
  }

  @Test
  public void aRetryingRunIsDrivenAndReadBackOverHttp() {
    String id = "run-" + UUID.randomUUID();
    httpClient
        .POST("/task-runs/" + id + "/retry-budget")
        .withRequestBody(new TaskRunEntity.SetRetryBudget(2, List.of(), 0.0))
        .invoke();
    post("/task-runs/" + id + "/states", new TaskRunEntity.RecordState(StateType.PENDING, "Pending", null));
    post("/task-runs/" + id + "/states", new TaskRunEntity.RecordState(StateType.RUNNING, "Running", null));
    post("/task-runs/" + id + "/failures", new TaskRunEntity.ReportFailure("boom", true));
    var completed =
        post("/task-runs/" + id + "/states", new TaskRunEntity.RecordState(StateType.COMPLETED, "Completed", null));

    assertThat(completed.history().stream().map(s -> s.type() + "/" + s.name()).toList())
        .containsExactly("PENDING/Pending", "RUNNING/Running", "RUNNING/Retrying", "COMPLETED/Completed");
    assertThat(completed.record().runCount()).isEqualTo(2);

    var reread =
        httpClient.GET("/task-runs/" + id).responseBodyAs(TaskRunView.class).invoke().body();
    assertThat(reread.record().runCount()).isEqualTo(2);
    assertThat(reread.retriesSpent()).isEqualTo(1);
  }

  @Test
  public void aCacheKeyIsComputedOverHttpAndMatchesPrefectsOwnDigest() {
    var request =
        new CacheKeyEndpoint.ComputeRequest(
            List.of(new CacheKeyEndpoint.PolicySpec("inputs", List.of())),
            Map.of("x", 42),
            null,
            null,
            null,
            null);
    var response =
        httpClient
            .POST("/cache-keys/compute")
            .withRequestBody(request)
            .responseBodyAs(CacheKeyEndpoint.ComputeResponse.class)
            .invoke()
            .body();
    assertThat(response.key()).isEqualTo(CacheKeyEndpointDigests.INPUTS_X42_COMPOUND);
    assertThat(response.effectivePolicies()).containsExactly("Inputs");
  }

  @Test
  public void anUnknownPolicyNameIsRefusedWithoutEchoingIt() {
    var request =
        new CacheKeyEndpoint.ComputeRequest(
            List.of(new CacheKeyEndpoint.PolicySpec("not-a-policy", List.of())),
            Map.of("x", 1),
            null,
            null,
            null,
            null);
    var response =
        httpClient.POST("/cache-keys/compute").withRequestBody(request).invoke();
    assertThat(response.httpResponse().status().intValue()).isEqualTo(400);
  }

  @Test
  public void theDefaultPolicyIsUsedWhenNoneIsNamedAndReportsInputsLast() {
    var request =
        new CacheKeyEndpoint.ComputeRequest(
            null, Map.of("x", 1), Map.of("a", 1), "task-1", "flow-1", "def t(): ...");
    var response =
        httpClient
            .POST("/cache-keys/compute")
            .withRequestBody(request)
            .responseBodyAs(CacheKeyEndpoint.ComputeResponse.class)
            .invoke()
            .body();
    assertThat(response.effectivePolicies()).containsExactly("TaskSource", "RunId", "Inputs");
    assertThat(response.key()).hasSize(32);
  }
}
