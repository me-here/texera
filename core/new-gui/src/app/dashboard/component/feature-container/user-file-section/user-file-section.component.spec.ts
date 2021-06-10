import { ComponentFixture, TestBed, inject, waitForAsync } from '@angular/core/testing';

import { NgbModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { CustomNgMaterialModule } from '../../../../common/custom-ng-material.module';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatListModule } from '@angular/material/list';

import { UserFileSectionComponent } from './user-file-section.component';
import { UserFileService } from '../../../../common/service/user/user-file/user-file.service';
import { UserService } from '../../../../common/service/user/user.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

describe('UserFileSectionComponent', () => {
  let component: UserFileSectionComponent;
  let fixture: ComponentFixture<UserFileSectionComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [UserFileSectionComponent],
      providers: [
        NgbModal,
        UserFileService,
        UserService
      ],
      imports: [
        CustomNgMaterialModule,
        NgbModule,
        FormsModule,
        ReactiveFormsModule,
        MatListModule,
        HttpClientTestingModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserFileSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', inject([HttpTestingController],
    (httpMock: HttpTestingController) => {
      expect(component).toBeTruthy();
    }));
});
