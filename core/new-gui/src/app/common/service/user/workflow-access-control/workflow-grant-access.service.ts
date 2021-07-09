import {HttpClient} from '@angular/common/http';
import {Injectable} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {AppSettings} from '../../../app-setting';
import {Workflow} from '../../../type/workflow';

export const WORKFLOW_ACCESS_URL = 'workflowaccess';

@Injectable({
  providedIn: 'root'
})
export class WorkflowGrantAccessService {
  constructor(private http: HttpClient) {
  }

  /**
   * persists a workflow to backend database and returns its updated information (e.g., new wid)
   * @param workflow
   * @param username
   * @param accessType
   */
  public grantWorkflowAccess(workflow: Workflow, username: string, accessType: string): Observable<String> {
    return this.http.post<String>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_URL}/share/${workflow.wid}/${username}/${accessType}`, null);
  }

  public getSharedAccess(workflow: Workflow): Observable<Map<string, string>> {
    return this.http.get<Map<string, string>>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_URL}/currentShare/${workflow.wid}`);
  }

  public removeAccess(workflow: Workflow, username: string): Observable<String> {
    return this.http.post<String>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_URL}/remove/${workflow.wid}/${username}`, null);
  }

  public testConnection(): void {
    console.log("connected");
  }
}
