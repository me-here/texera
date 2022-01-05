import { Injectable } from '@angular/core';
import { webSocket } from 'rxjs/webSocket';
import { CommandMessage} from '../workflow-graph/model/workflow-action.service';
import { environment } from '../../../../environments/environment';
import { Subject, Observable } from 'rxjs';

@Injectable({
  providedIn: "root",
})

// This service will be for workflow collab
// First step: move the arrays from undo/redo service into Yjs doc.
// Once we have some stuff set I'll send the changes between clients and just see how to work with them.
// Need to find a way to push as part of change, not update like that

// How do we recover if the connection gets dropped?
export class WorkflowCollabService {
  private sendData: boolean = false; // set initial value to false to disable service

  private socket = webSocket({
    url: this.getWorkflowWebsocketUrl(),
    deserializer: msg => msg["data"],
  });

  // maybe create a subject for testing?
  private messageSubject: Subject<CommandMessage> = new Subject<CommandMessage>();

  constructor() {
    const self = this;
    if (environment.enableWorkflowCollab) {
      // to disable service, change to true to enable
      // Noticed a strange interaction between this feature and SaveWorkflowService, things will get duplicated
      // When enabling this feature, am currently disabling that feature
      this.setSendData(true);
      this.socket.subscribe({
        next(response) {
          const message = JSON.parse(response) as CommandMessage;
          self.messageSubject.next(message);
        },
        error(err) {
          throw new Error(err);
        },
        complete() {
          console.log("websocket finished and disconnected");
        },
      });
    }
  }

  public setSendData(toggle: boolean): void {
    this.sendData = toggle;
  }

  public getSendData(): boolean {
    return this.sendData;
  }

  public sendCommand(update: string): void {
    this.socket.next(JSON.parse(update));
  }

  public getCommandMessageStream(): Observable<CommandMessage> {
    return this.messageSubject.asObservable();
  }

  public getWorkflowWebsocketUrl(): string {
    const websocketUrl = new URL("automerge", document.baseURI);
    // replace protocol, so that http -> ws, https -> wss
    websocketUrl.protocol = websocketUrl.protocol.replace("http", "ws");
    return websocketUrl.toString();
  }
}
