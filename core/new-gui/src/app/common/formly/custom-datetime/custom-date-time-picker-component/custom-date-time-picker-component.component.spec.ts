import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CustomDateTimePickerComponentComponent } from './custom-date-time-picker-component.component';

describe('CustomDateTimePickerComponentComponent', () => {
  let component: CustomDateTimePickerComponentComponent;
  let fixture: ComponentFixture<CustomDateTimePickerComponentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CustomDateTimePickerComponentComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CustomDateTimePickerComponentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
