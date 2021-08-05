import { Injectable } from '@angular/core';
import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from '../execute-workflow/execute-workflow.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { ExecutionState } from '../../types/execute-workflow.interface';

@Injectable({
  providedIn: 'root'
})
export class PropertyEditService {

  // debounce time for form input in milliseconds
  //  please set this to multiples of 10 to make writing tests easy
  public static formInputDebounceTime: number = FORM_DEBOUNCE_TIME_MS;

  // re-declare enum for angular template to access it
  public readonly ExecutionState = ExecutionState;

  constructor(
    private workflowActionService: WorkflowActionService,
    private executeWorkflowService: ExecuteWorkflowService
  ) { }


  public hasBreakpoint(currentLinkId: string | undefined): boolean {
    if (!currentLinkId) {
      return false;
    }
    return this.workflowActionService.getTexeraGraph().getLinkBreakpoint(currentLinkId) !== undefined;
  }

  public handleAddBreakpoint(currentLinkId: string | undefined, formData: any) {
    if (currentLinkId && this.workflowActionService.getTexeraGraph().hasLinkWithID(currentLinkId)) {
      this.workflowActionService.setLinkBreakpoint(currentLinkId, formData);
      if (this.executeWorkflowService.getExecutionState().state === ExecutionState.Paused ||
        this.executeWorkflowService.getExecutionState().state === ExecutionState.BreakpointTriggered) {
        this.executeWorkflowService.addBreakpointRuntime(currentLinkId, formData);
      }
    }
  }

  /**
   * This method handles the link breakpoint remove button click event.
   * It will hide the property editor, clean up currentBreakpointInitialData.
   * Then unhighlight the link and remove it from the workflow.
   */
  public removeBreakpoint(currentLinkId: string | undefined) {
    if (currentLinkId) {
      // remove breakpoint in texera workflow first, then unhighlight it
      this.workflowActionService.removeLinkBreakpoint(currentLinkId);
      this.workflowActionService.getJointGraphWrapper().unhighlightLinks(currentLinkId);
    }

  }

  public confirmChangeOperatorLogic(currentOperatorId: string | undefined) {

    if (currentOperatorId) {
      this.executeWorkflowService.changeOperatorLogic(currentOperatorId);
    }
  }

  public isPausedState(currentOperatorId: string | undefined): boolean {
    return currentOperatorId !== undefined &&
      (this.executeWorkflowService.getExecutionState().state === ExecutionState.Paused ||
        this.executeWorkflowService.getExecutionState().state === ExecutionState.BreakpointTriggered);
  }


}
