import { Component, OnInit } from '@angular/core';
import { FieldWrapper, FormlyFieldConfig } from '@ngx-formly/core';
import { cloneDeep, isEqual, merge } from 'lodash';
import { Preset, PresetService } from 'src/app/workspace/service/preset/preset.service';
import { asType } from '../../util/assert';

export interface PresetKey {
  presetType: string;
  saveTarget: string;
  applyTarget: string;
}

@Component({
templateUrl: './preset-wrapper.component.html',
styleUrls: ['./preset-wrapper.component.scss'],
})
export class PresetWrapperComponent extends FieldWrapper implements OnInit {

  public searchResults: Preset[] = [];
  private searchTerm: string = '';
  private presetType: string = '';
  private saveTarget: string = '';
  private applyTarget: string = '';

  constructor( private presetService: PresetService ) { super(); }

  ngOnInit(): void {
    if (
      this.field.key === undefined ||
      typeof this.field.key !== 'string' ||
      this.field.templateOptions === undefined ||
      this.field.templateOptions.presetKey === undefined) {

      throw Error(`form preset-wrapper field ${this.field} doesn't contain necessary .key and .templateOptions.presetKey attributes`);
    }
    const presetKey = <PresetKey> this.field.templateOptions.presetKey;
    this.searchTerm = this.formControl.value !== null ? this.formControl.value : '';
    this.presetType = presetKey.presetType;
    this.saveTarget =  presetKey.saveTarget;
    this.applyTarget = presetKey.applyTarget;
    this.searchResults = this.getSearchResults(this.presetService.getPresets(this.presetType, this.saveTarget), this.searchTerm, true);

    this.handleSavePresets();
    this.handleFieldValueChanges();
  }

  public applyPreset(preset: Preset) {
    this.presetService.applyPreset(this.presetType, this.applyTarget, preset);
  }

  public deletePreset(preset: Preset) {
    const presets = cloneDeep(this.presetService.getPresets(this.presetType, this.saveTarget))
      .filter(oldPreset => !isEqual(oldPreset, preset));

    this.presetService.savePresets(this.presetType, this.saveTarget, presets, `Deleted preset: ${this.getEntryTitle(preset)}`, 'error');
  }

  public getEntryTitle(preset: Preset): string {
    return preset[asType(this.field.key, 'string')].toString();
  }

  public getEntryDescription(preset: Preset): string {
    return Object.keys(preset).filter(key => key !== asType(this.field.key, 'string')).map(key => preset[key]).join(', ');
  }

  public getSearchResults(presets: Readonly<Preset[]>, searchTerm: string, showAllResults: boolean): Preset[] {
    if (showAllResults) {
      return presets.slice();
    } else {
      return presets.filter(
        preset => this.getEntryTitle(preset)
          .replace(/^\s+|\s+$/g, '').toLowerCase().startsWith(searchTerm.toLowerCase())
      );
    }
  }

  public onDropdownVisibilityEvent(visible: boolean) {
    if (visible) {
      this.searchResults = this.getSearchResults(this.presetService.getPresets(this.presetType, this.saveTarget), this.searchTerm, true);
    }
  }

  private handleSavePresets() {
    this.presetService.savePresetsStream
      .filter((presets) => presets.type === this.presetType && presets.target === this.saveTarget)
      .subscribe({
        next: (saveEvent) => {
          this.searchResults = this.getSearchResults(saveEvent.presets, this.searchTerm, false);
        }
      });
  }

  private handleFieldValueChanges() {
    this.formControl.valueChanges.subscribe({
      next: (value: string | number | boolean ) => {
        this.searchTerm = value.toString();
        this.searchResults = this.getSearchResults(this.presetService.getPresets(this.presetType, this.saveTarget), this.searchTerm, false);
      }
    });
  }

  public static setupFieldConfig( config: FormlyFieldConfig, presetType: string, saveTarget: string, applyTarget: string) {
    const fieldConfig: FormlyFieldConfig = {
      wrappers: ['form-field', 'preset-wrapper'], // wrap form field in default theme and then preset wrapper
      templateOptions: {
        presetKey: <PresetKey> {
          presetType: presetType,
          saveTarget: saveTarget,
          applyTarget: applyTarget
        }
      }
    };
    merge(config, fieldConfig);
  }

}
