import { Injectable } from "@angular/core";
import { environment } from "../../../../environments/environment";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { merge } from "rxjs";
import { ResultExportResponse } from "../../types/workflow-websocket.interface";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { filter } from "rxjs/operators";

@Injectable({
  providedIn: "root",
})
export class WorkflowResultExportService {
  hasResultToExport: boolean = false;
  exportExecutionResultEnabled: boolean = environment.exportExecutionResultEnabled;

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    this.registerResultExportResponseHandler();
    this.registerResultToExportUpdateHandler();
  }

  registerResultExportResponseHandler() {
    this.workflowWebsocketService
      .subscribeToEvent("ResultExportResponse")
      .subscribe((response: ResultExportResponse) => {
        if (response.status === "success") {
          this.notificationService.success(response.message);
        } else {
          this.notificationService.error(response.message);
        }
      });
  }

  registerResultToExportUpdateHandler() {
    merge(
      this.executeWorkflowService
        .getExecutionStateStream()
        .pipe(filter(({ previous, current }) => current.state === ExecutionState.Completed)),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream()
    ).subscribe(() => {
      this.hasResultToExport =
        this.executeWorkflowService.getExecutionState().state === ExecutionState.Completed &&
        this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs()
          .filter(operatorId =>
            this.workflowActionService
              .getTexeraGraph()
              .getOperator(operatorId)
              .operatorType.toLowerCase()
              .includes("sink")
          ).length > 0;
    });
  }

  /**
   * export the workflow execution result according the export type
   */
  exportWorkflowExecutionResult(exportType: string, workflowName: string): void {
    if (!environment.exportExecutionResultEnabled || !this.hasResultToExport) {
      return;
    }

    this.notificationService.loading("exporting...");
    this.workflowActionService
      .getJointGraphWrapper()
      .getCurrentHighlightedOperatorIDs()
      .filter(operatorId =>
        this.workflowActionService.getTexeraGraph().getOperator(operatorId).operatorType.toLowerCase().includes("sink")
      )
      .forEach(operatorId => {
        this.workflowWebsocketService.send("ResultExportRequest", {
          exportType,
          workflowName,
          operatorId,
        });
      });
  }
}
