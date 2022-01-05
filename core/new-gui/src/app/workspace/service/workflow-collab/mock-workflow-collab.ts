import { CommandMessage } from '../workflow-graph/model/workflow-action.service';
import {
  mockScanPredicate,
  mockPoint
} from '../workflow-graph/model/mock-workflow-data';

export const MockCommandMessage: CommandMessage = {
  action: 'addOperator',
  parameters: [mockScanPredicate, mockPoint],
  type: 'execute'
};
