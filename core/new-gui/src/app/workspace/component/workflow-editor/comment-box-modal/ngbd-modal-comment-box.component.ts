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

    public savedComments: string[] = [];

    public commentForm = this.formBuilder.group({
        comment: ['', [Validators.required]]
    });
    constructor(
        public workflowActionService: WorkflowActionService,
        public activeModal: NgbActiveModal,
        private formBuilder: FormBuilder
    ) {}
    ngOnInit(): void {
        this.refreshSavedComments(this.commentBox.comments);
    }

    public refreshSavedComments(comments: string[]): void {
        this.savedComments = comments;
    }

    public onClickAddComment(): void {
        if (this.commentForm.get('comment')?.invalid) {
            alert('Cannot Submit Empty Comment!!');
            return;
        }
        const newComment = this.commentForm.get('comment')?.value;
        this.updateComments(newComment);
    }

    public updateComments(newComment: string): void {
        this.savedComments.push(newComment);
        this.refreshSavedComments(this.savedComments);
    }

}

