import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BreakpointControlPanelComponent } from './breakpoint-control-panel.component';

describe('BreakpointControlPanelComponent', () => {
  let component: BreakpointControlPanelComponent;
  let fixture: ComponentFixture<BreakpointControlPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BreakpointControlPanelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BreakpointControlPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
