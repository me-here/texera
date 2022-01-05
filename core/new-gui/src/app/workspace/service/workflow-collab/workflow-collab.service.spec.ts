import { TestBed } from '@angular/core/testing';
import * as Rx from 'rxjs';
import * as RxJSWebSocket from 'rxjs/webSocket';
import { WorkflowCollabService } from './workflow-collab.service';
import { CommandMessage} from '../workflow-graph/model/workflow-action.service';
import { MockCommandMessage } from './mock-workflow-collab';
import { environment } from '../../../../environments/environment';

describe('WorkflowCollabService', () => {
  let workflowCollabService: WorkflowCollabService;
  let mockBackend: Rx.Subject<string>;
  let backendTester: Rx.Subject<string>;
  beforeEach(() => {
    function mockConnect(url: string) {
      mockBackend = new Rx.Subject<string>();
      backendTester = new Rx.Subject<string>();
      const observable = Rx.Observable.create((obs: Rx.Observer<string>) => {
        mockBackend.next = obs.next.bind(obs);
        mockBackend.error = obs.error.bind(obs);
        mockBackend.complete = obs.complete.bind(obs);
        return mockBackend.unsubscribe.bind(mockBackend);
      });
      const observer = {
        next: (data: Object) => {
          backendTester.next(JSON.stringify(data));
        }
      };
      return Rx.Subject.create(observer, observable);
    }
    TestBed.configureTestingModule({
      providers: [
        WorkflowCollabService,
      ]
    });

    const funcSpy = jasmine.createSpy('webSocket').and.returnValue(mockConnect('abc'));
    spyOnProperty(RxJSWebSocket, 'webSocket', 'get').and.returnValue(funcSpy);
    workflowCollabService = TestBed.get(WorkflowCollabService);
  });

  it('should be created', () => {
    const service: WorkflowCollabService = TestBed.get(WorkflowCollabService);
    expect(service).toBeTruthy();
  });

  it('should send command to websocket', (done: DoneFn) => {
    const mockCommand = MockCommandMessage;
    backendTester.subscribe(
      (s: string) => {
        // Mimic cleanup done on string by websocket
        s = s.replace(/\\/g, '');
        s = '{' + s.substring(1, s.length - 1) + '}';
        expect(s).toEqual(expectedResponse);
        done();
      },
      () => {},
      () => {}
    );
    const expectedResponse = JSON.stringify(mockCommand);
    workflowCollabService.sendCommand(expectedResponse);
  });

  it('should return a observable of received message', () => {
    const test = workflowCollabService.getCommandMessageStream();
    expect(test).toBeDefined();
  });

  // Think about what tests we should include
  // Need to mimic communication between clients
  it('command should successfully be received', () => {
    expect(true).toBeTruthy();
  });

  it('should receive responses from the backend and emit response', (done: DoneFn) => {
    if (environment.enableWorkflowCollab) {
      const stream = workflowCollabService.getCommandMessageStream();
      stream.subscribe(
        (mess: CommandMessage) => {
          expect(JSON.stringify(mess)).toEqual(expectedResponse);
          done();
        },
        () => {},
        () => {}
      );
      const expectedResponse = JSON.stringify(MockCommandMessage);
      mockBackend.next(expectedResponse);
    } else {
      expect(true).toBeTruthy();
      done();
    }

    // need to remove this test when service is disabled
  });

});
