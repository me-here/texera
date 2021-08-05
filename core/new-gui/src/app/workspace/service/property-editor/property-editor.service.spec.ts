import { TestBed } from '@angular/core/testing';

import { PropertyEditService } from './property-edit.service';

describe('PropertyEditorService', () => {
  let service: PropertyEditService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(PropertyEditService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
