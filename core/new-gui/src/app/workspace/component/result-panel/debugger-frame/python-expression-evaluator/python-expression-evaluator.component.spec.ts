import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PythonExpressionEvaluatorComponent } from './python-expression-evaluator.component';

describe('PythonExpressionEvaluatorComponent', () => {
  let component: PythonExpressionEvaluatorComponent;
  let fixture: ComponentFixture<PythonExpressionEvaluatorComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ PythonExpressionEvaluatorComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PythonExpressionEvaluatorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
