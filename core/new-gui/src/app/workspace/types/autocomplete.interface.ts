import { JSONSchema4 } from 'json-schema';

/**
 * The type decalaration of the response sent by **backend** when
 * asking for source table names.
 */
export interface SourceTableNamesAPIResponse extends Readonly < {
  code: number,
  message: string
} > { }

export interface SourceTableDetails extends Readonly <{
  tableName: string,
  schema: SourceTableSchema
}> { }

export interface SourceTableSchema extends Readonly<{
  attributes: ReadonlyArray<SourceTableAttribute>
}> { }

export interface SourceTableAttribute extends Readonly <{
  attributeName: string,
  attributeType: string
}> { }

/**
 * The backend interface of the return object of a successful execution
 * of autocomplete API
 */
export interface AutocompleteSucessResult extends Readonly<{
  code: 0,
  result: JSONSchema4
}> { }

/**
 * The backend interface of the return object of a failed execution of
 * autocomplete API
 */
export interface AutocompleteErrorResult extends Readonly< {
  code: -1,
  message: string
}> { }

/**
 * Discriminated Union
 * http://www.typescriptlang.org/docs/handbook/advanced-types.html
 *
 * AutocompleteResult type can be either AutocompleteSucessResult or AutocompleteErrorResult.
 *  but cannot contain both structures at the same time.
 * In this case:
 *  if the code value is 0, then the object type must be AutocompleteSucessResult
 *  if the code value is -1, then the object type must be AutocompleteErrorResult
 */
export type AutocompleteResult = AutocompleteSucessResult | AutocompleteErrorResult;