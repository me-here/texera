import { Component } from '@angular/core';
import { FieldType } from '@ngx-formly/core';


@Component({
  selector: 'texera-custom-date-time-picker-component',
  templateUrl: './custom-date-time-picker-component.component.html',
  styleUrls: ['./custom-date-time-picker-component.component.scss']
})
export class CustomDateTimePickerComponentComponent extends FieldType {

  constructor() {
    super();
  }


}
