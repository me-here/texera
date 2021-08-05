import { Component, OnInit } from '@angular/core';
import { PropertyEditService } from '../../../service/property-editor/property-edit.service';
import { ExecuteWorkflowService } from '../../../service/execute-workflow/execute-workflow.service';

@Component({
  selector: 'texera-breakpoint-control-panel',
  templateUrl: './breakpoint-control-panel.component.html',
  styleUrls: ['./breakpoint-control-panel.component.scss']
})
export class BreakpointControlPanelComponent implements OnInit {


  constructor(public propertyEditService: PropertyEditService,
              private executeWorkflowService: ExecuteWorkflowService) { }

  ngOnInit(): void {
  }

  public handleRemoveBreakpoint() {
    this.propertyEditService.removeBreakpoint(this.propertyEditService.currentLinkID);
    this.propertyEditService.clearPropertyEditor();
  }

  public allowChangeOperatorLogic() {
    this.propertyEditService.setInteractivity(true);
  }

  public confirmChangeOperatorLogic() {
    this.propertyEditService.setInteractivity(false);
    if (this.propertyEditService.currentOperatorID) {
      this.executeWorkflowService.changeOperatorLogic(this.propertyEditService.currentOperatorID);
    }
  }
}
