import { Component, Input, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormBuilder, Validators } from '@angular/forms';
import { CommentBox } from 'src/app/workspace/types/workflow-common.interface';
import { WorkflowActionService } from 'src/app/workspace/service/workflow-graph/model/workflow-action.service';

@Component({
    selector: 'texera-ngbd-modal-comment-box',
    templateUrl: './ngbd-modal-comment-box.component.html',
    styleUrls: ['./ngbd-modal-comment-box.component.scss']
})
export class NgbdModalCommentBoxComponent implements OnInit {
    @Input() commentBox!: CommentBox;

    constructor(
        public workflowActionService: WorkflowActionService,
        public activeModal: NgbActiveModal
    ){};

    ngOnInit() : void {
        console.log("CommentBoxModalSetUpDone");
    }

}

