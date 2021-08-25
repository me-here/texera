import { Component, Input, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormBuilder, Validators } from '@angular/forms';
import { CommentBox, Comment } from 'src/app/workspace/types/workflow-common.interface';
import { WorkflowActionService } from 'src/app/workspace/service/workflow-graph/model/workflow-action.service';
import { UserService } from 'src/app/common/service/user/user.service';


@Component({
    selector: 'texera-ngbd-modal-comment-box',
    templateUrl: './ngbd-modal-comment-box.component.html',
    styleUrls: ['./ngbd-modal-comment-box.component.scss']
})
export class NgbdModalCommentBoxComponent {
    @Input() commentBox!: CommentBox;

    public commentForm = this.formBuilder.group({
        comment: ['', [Validators.required]]
    });
    constructor(
        public workflowActionService: WorkflowActionService,
        public activeModal: NgbActiveModal,
        private formBuilder: FormBuilder,
        public userService: UserService
    ) {}

    public onClickAddComment(): void {
        if (this.commentForm.get('comment')?.invalid) {
            alert('Cannot Submit Empty Comment!!');
            return;
        }
        const newComment = this.commentForm.get('comment')?.value;
        this.updateComments(newComment);
        this.commentForm.get('comment')?.setValue('');
    }

    public updateComments(newComment: string): void {
        const currentTime: string = new Date().toISOString();
        const creator = this.userService.getUser()?.name;
        this.workflowActionService.addComment(
          {content: newComment, creationTime: currentTime, creator: creator},
          this.commentBox.commentBoxID
        );
        console.log(this.commentBox);
    }

}

