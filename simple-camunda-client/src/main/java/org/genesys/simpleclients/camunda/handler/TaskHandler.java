package org.genesys.simpleclients.camunda.handler;

import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskHandler;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.genesys.simpleclients.camunda.common.Status;

@Slf4j
public abstract class TaskHandler implements ExternalTaskHandler {

  private static final String LOG_FORMAT = "ProcessInstanceId={} TaskId={} Status={}";

  @Override
  public final void execute(final ExternalTask externalTask,
    final ExternalTaskService externalTaskService) {

    log.info(LOG_FORMAT, externalTask.getProcessInstanceId(), externalTask.getId(), Status.STARTED);
    try {
      handle(externalTask, externalTaskService);
      log.info(LOG_FORMAT, externalTask.getProcessInstanceId(), externalTask.getId(),
        Status.COMPLETED);
    } catch (final Exception e) {
      externalTaskService.handleBpmnError(externalTask, e.getMessage());
      log.error(LOG_FORMAT, externalTask.getProcessInstanceId(), externalTask.getId(), Status.ERROR,
        e);
    }
  }

  protected abstract void handle(final ExternalTask externalTask,
    final ExternalTaskService externalTaskService);
}
