import { Injectable } from '@angular/core';
import { environment } from '../../../../environments/environment';
import { WebPaginationUpdate } from '../../types/execute-workflow.interface';
import { ExecuteWorkflowService } from '../execute-workflow/execute-workflow.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';


class OperatorPaginationResultService {



  constructor(operatorID: string) {

  }

  public onPaginationUpdate(update: WebPaginationUpdate): void {

  }

}

class OperatorResultService {
  constructor(operatorID: string) {

  }
}


@Injectable({
  providedIn: 'root'
})
export class WorkflowResultService {

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== 'WebResultUpdateEvent') {
        return;
      }
      const a = event;
      // if (event.ty)
    });
  }
}
