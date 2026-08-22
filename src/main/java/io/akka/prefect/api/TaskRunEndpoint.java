package io.akka.prefect.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpResponses;
import io.akka.prefect.application.TaskRunEntity;

/** A task run's own surface — set its retry budget, record its states, report a failed attempt. */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/task-runs")
public class TaskRunEndpoint {

  private final ComponentClient componentClient;

  public TaskRunEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  @Post("/{taskRunId}/retry-budget")
  public HttpResponse setRetryBudget(String taskRunId, TaskRunEntity.SetRetryBudget budget) {
    componentClient
        .forEventSourcedEntity(taskRunId)
        .method(TaskRunEntity::setRetryBudget)
        .invoke(budget);
    return HttpResponses.ok();
  }

  @Post("/{taskRunId}/states")
  public TaskRunView recordState(String taskRunId, TaskRunEntity.RecordState command) {
    return TaskRunView.from(
        componentClient
            .forEventSourcedEntity(taskRunId)
            .method(TaskRunEntity::recordState)
            .invoke(command));
  }

  @Post("/{taskRunId}/state-batches")
  public TaskRunView recordStates(String taskRunId, java.util.List<TaskRunEntity.RecordState> commands) {
    return TaskRunView.from(
        componentClient
            .forEventSourcedEntity(taskRunId)
            .method(TaskRunEntity::recordStates)
            .invoke(commands));
  }

  @Post("/{taskRunId}/failures")
  public TaskRunView reportFailure(String taskRunId, TaskRunEntity.ReportFailure report) {
    return TaskRunView.from(
        componentClient
            .forEventSourcedEntity(taskRunId)
            .method(TaskRunEntity::reportFailure)
            .invoke(report));
  }

  @Get("/{taskRunId}")
  public TaskRunView get(String taskRunId) {
    return TaskRunView.from(
        componentClient.forEventSourcedEntity(taskRunId).method(TaskRunEntity::get).invoke());
  }
}
