import {Component, Input, OnInit} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';
import {FormBuilder, Validators} from "@angular/forms";
import {WorkflowGrantAccessService} from "../../../../../common/service/user/workflow-access-control/workflow-grant-access.service";
import {Workflow} from "../../../../../common/type/workflow";


@Component({
  selector: 'texera-ngbd-modal-share-access',
  templateUrl: './ngbd-modal-share-access.component.html',
  styleUrls: ['./ngbd-modal-share-access.component.scss']
})
export class NgbdModalShareAccessComponent implements OnInit {

  @Input() workflow!: Workflow

  shareForm = this.formBuilder.group({
    username: '',
    accessType: ['', [Validators.required]]
  });


  accessTypes: any = ["read", "write"]

  currentShare: Map<string, string> = new Map<string, string>()

  sharedUsers: string[] = []

  public defaultWeb: String = 'http://localhost:4200/';

  constructor(
    public activeModal: NgbActiveModal,
    private workflowGrantAccessService: WorkflowGrantAccessService,
    private formBuilder: FormBuilder
  ) {
  }

  ngOnInit(): void {
    console.log(this.workflow)
    this.onClickGetAllSharedAccess(this.workflow)
  }

  public onClickGetAllSharedAccess(workflow: Workflow): void {
    this.workflowGrantAccessService.getSharedAccess(workflow).subscribe(res => {
      this.currentShare = new Map(Object.entries(res));
      this.sharedUsers = []
      for (let username of this.currentShare.keys()) {
        this.sharedUsers.push(username);
      }
    });
  }

  public onClickShareWorkflow(workflow: Workflow, userToShareWith: string, accessType: string): void {
    this.workflowGrantAccessService.grantWorkflowAccess(workflow, userToShareWith, accessType).subscribe(resp => this.onClickGetAllSharedAccess(workflow));
  }

  public onClickRemoveAccess(workflow: Workflow, userToRemove: string): void {
    this.workflowGrantAccessService.removeAccess(workflow, userToRemove).subscribe(res => {
      console.log(res)
      this.onClickGetAllSharedAccess(workflow);
    });
  }

  changeType(e: any) {
    console.log(e.value)
    this.shareForm.setValue(["accessType"], e.target.value)
  }


  onSubmit(workflow: Workflow): any {
    this.onClickShareWorkflow(workflow, this.shareForm.get("username")?.value, this.shareForm.get("accessType")?.value)
  }


}
