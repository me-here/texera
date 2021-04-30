import { Injectable } from '@angular/core';
import * as Ajv from 'ajv';
import { cloneDeep, has, isEqual, merge, pickBy } from 'lodash';
import { NzMessageService } from 'ng-zorro-antd/message';
import { Observable, Subject } from 'rxjs';
import { DictionaryService, JSONValue, UserDictionary } from 'src/app/common/service/user/user-dictionary/dictionary.service';
import { isType } from 'src/app/common/util/assert';
import { CustomJSONSchema7 } from '../../types/custom-json-schema.interface';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';

type AlertMessageType = 'success' | 'error' | 'info' | 'warning';

export type Preset = {[key: string]: string|number|boolean};

@Injectable({
  providedIn: 'root'
})
export class PresetService {
  private static DICT_PREFIX = 'Preset';
  private static ajvStrip = new Ajv({ useDefaults: true, removeAdditional: true});

  public readonly applyPresetStream: Observable<{type: string, target: string, preset: Preset}>;
  public readonly savePresetsStream: Observable<{type: string, target: string, presets: Preset[]}>;
  public presetDict: UserDictionary;

  private applyPresetSubject = new Subject<{type: string, target: string, preset: Preset}>();
  private savePresetSubject = new Subject<{type: string, target: string, presets: Preset[]}>();

  private ajv = new Ajv();

  constructor(
    private dictionaryService: DictionaryService,
    private messageService: NzMessageService,
    private workflowActionService: WorkflowActionService,
    private operatorMetadataService: OperatorMetadataService,
    ) {
    this.applyPresetStream = this.applyPresetSubject.asObservable();
    this.savePresetsStream = this.savePresetSubject.asObservable();
    this.presetDict = this.getPresetDict();

    this.handleApplyOperatorPresets();
  }

  public applyPreset(type: string, target: string, preset: Preset) {
    this.applyPresetSubject.next({type: type, target: target, preset: preset});
  }

  public savePresets(type: string, target: string, presets: Preset[],
    displayMessage?: string|null, messageType: AlertMessageType = 'success') {

    if (presets.length > 0) {
      this.presetDict[`${type}-${target}`] = presets;
    } else {
      delete this.presetDict[`${type}-${target}`];
    }
    this.savePresetSubject.next({type: type, target: target, presets: presets});
    this.displaySavePresetMessage(messageType, displayMessage);
  }

  public getPresets(type: string, target: string): Readonly<Preset[]> {
    const presets = this.presetDict[`${type}-${target}`] ?? [];
    if (this.isPresetArray(presets)) {
      return presets;
    } else {
      throw new Error(`stored preset data ${presets} is formatted incorrectly`);
    }
  }

  public isValidOperatorPreset(preset: Preset, operatorID: string): boolean {
    const presetSchema = PresetService.getOperatorPresetSchema(
      this.operatorMetadataService.getOperatorSchema(
        this.workflowActionService.getTexeraGraph().getOperator(operatorID).operatorType).jsonSchema);
    const fitsSchema = this.ajv.compile(presetSchema)(preset);
    const noEmptyProperties = Object.keys(preset).every(
      (key: string) => !isType(preset[key], 'string') || ((<string> preset[key]).trim()).length > 0);

    return fitsSchema && noEmptyProperties;
  }

  public isValidNewOperatorPreset(preset: Preset, operatorID: string): boolean {
    const isNewPreset = !this.getPresets('operator', this.workflowActionService.getTexeraGraph().getOperator(operatorID).operatorType)
      .some(existingPreset => isEqual(preset, existingPreset));

    return isNewPreset && this.isValidOperatorPreset(preset, operatorID);
  }

  private getPresetDict(): UserDictionary {
    const dict = this.dictionaryService.forceGetUserDictionary();
    return new Proxy(dict, {
      get(target: UserDictionary, key: symbol) {
        return target[`${PresetService.DICT_PREFIX}-${key.toString()}`];
      },
      set(target: UserDictionary, key: string, value: JSONValue) {
        target[`${PresetService.DICT_PREFIX}-${key}`] = value;
        return true;
      },
      deleteProperty(target: UserDictionary, key: string) {
        delete target[`${PresetService.DICT_PREFIX}-${key}`];
        return true;
      },
      defineProperty(target: UserDictionary, key: string, value: JSONValue) {
        target[`${PresetService.DICT_PREFIX}-${key}`] = value;
        return true;
      },
      has(target: UserDictionary, key: string) {
        return `${PresetService.DICT_PREFIX}-${key}` in target;
      },
    });
  }

  private isPresetArray(presets: JSONValue): presets is Preset[] {
    if (!(Array.isArray(presets))) {
      throw new Error(`stored preset data ${presets} isn't an array`);
    } else if ((presets as Array<any>).some( preset => typeof preset !== 'object')) {
      throw new Error(`stored preset data ${presets} isn't an array of objects`);
    } else if ((presets as Array<any>).some( preset => Object.keys(preset).some(key => typeof preset[key] !== 'string'))) {
      throw new Error(`stored preset data ${presets} isn't an array of objects with only attributes of type string`);
    } else {
      return true;
    }
  }

  private displaySavePresetMessage(messageType: AlertMessageType, displayMessage?: string|null) {
    if (displayMessage === undefined) {
      switch (messageType) {
        case 'error':
          this.messageService.error('Preset deleted');
          break;
        case 'info':
          throw new Error(`no default save preset info message`);
          // break;
        case 'success':
          this.messageService.success('Preset saved');
          break;
        case 'warning':
          throw new Error(`no default save preset warning message`);
          // break;
      }
    } else if (displayMessage === null) {
      // do not display explicitly null message
      return;
    } else {
      switch (messageType) {
        case 'error':
          this.messageService.error(displayMessage);
          break;
        case 'info':
          this.messageService.info(displayMessage);
          break;
        case 'success':
          this.messageService.success(displayMessage);
          break;
        case 'warning':
          this.messageService.warning(displayMessage);
          break;
      }
    }
  }

  private handleApplyOperatorPresets() {
    this.applyPresetStream.subscribe({
      next: (applyEvent) => {
        if ( applyEvent.type === 'operator' && this.workflowActionService.getTexeraGraph().hasOperator(applyEvent.target)) {
          if (this.isValidOperatorPreset(applyEvent.preset, applyEvent.target)) {
            this.workflowActionService.setOperatorProperty(
              applyEvent.target,
              merge(cloneDeep(this.workflowActionService.getTexeraGraph().getOperator(applyEvent.target).operatorProperties),
                applyEvent.preset));
          } else {
            const schema = PresetService.getOperatorPresetSchema(
              this.operatorMetadataService.getOperatorSchema(
                this.workflowActionService.getTexeraGraph().getOperator(applyEvent.target).operatorType).jsonSchema);
            throw new Error(`Error applying preset: preset ${applyEvent.preset} was not a valid preset for ${applyEvent.target} with schema ${schema}`);
          }
        }
      }
    });
  }

  public static getOperatorPresetSchema(operatorSchema: CustomJSONSchema7): CustomJSONSchema7 {
    const copy = cloneDeep(operatorSchema);
    if (operatorSchema.properties === undefined) { throw new Error(`provided operator schema ${operatorSchema} has no properties`); }
    const properties = pickBy(copy.properties, (prop) => has(prop, 'enable-presets') && (prop as any)['enable-presets'] === true);
    if (isEqual(properties, {})) { throw new Error(`provided operator schema ${operatorSchema} has no preset properties`); }
    return {
      type: 'object',
      properties: properties,
      required: Object.keys(properties),
      additionalProperties: false,
    };
  }

  public static getOperatorPreset(operatorSchema: CustomJSONSchema7, operatorProperties: object) {
    const copy = cloneDeep(operatorProperties as Preset);
    const presetSchema = this.getOperatorPresetSchema(operatorSchema);
    const strip = this.ajvStrip.compile(presetSchema); // this validator also removes extra properties that aren't a part of the preset
    if (strip(copy)) {
      return copy;
    } else {
      throw new Error(`provided operator properties ${operatorProperties} does not conform to preset schema ${presetSchema}`);
    }
  }

  public static filterOperatorPresetProperties(operatorSchema: CustomJSONSchema7, operatorProperties: object) {
    const copy = cloneDeep(operatorProperties as Preset);
    const presetSchema = this.getOperatorPresetSchema(operatorSchema);
    const strip = this.ajvStrip.compile(presetSchema); // this validator also removes extra properties that aren't a part of the preset
    strip(copy);
    return copy;
  }
}
