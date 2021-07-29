import { Component } from '@angular/core';
import { FieldArrayType } from '@ngx-formly/core';

@Component({
  // selector: 'formly-array-type',
  template: `
    <div>
      <legend *ngIf="to.label">{{ to.label }}</legend>
      <p *ngIf="to.description">{{ to.description }}</p>

      <div class="alert alert-danger" role="alert" *ngIf="showError && formControl.errors">
        <formly-validation-message [field]="field"></formly-validation-message>
      </div>

      <div *ngFor="let field of field.fieldGroup;let i = index;" class="row">
        <formly-field [field]="field" class="formly-array-row"></formly-field>
        <button nz-button class="formly-array-button" (click)="remove(i)">
          <i nz-icon nzType="minus-circle" nzTheme="outline"></i>
        </button>
      </div>
      <button nz-button class="formly-array-button" (click)="add()">
        <i nz-icon nzType="plus-circle" nzTheme="outline"></i>
      </button>
    </div>
  `,
  styles: ['.formly-array-button {border-radius: 5px; border: none;}',
    // '.formly-array-row { width: 35%}'
  ]
})
export class ArrayTypeComponent extends FieldArrayType {
}


/**  Copyright 2018 Google Inc. All Rights Reserved.
 Use of this source code is governed by an MIT-style license that
 can be found in the LICENSE file at http://angular.io/license */
