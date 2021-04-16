import { HttpClient, HttpResponse } from '@angular/common/http';
import { TypeofExpr } from '@angular/compiler';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { AppSettings } from 'src/app/common/app-setting';
import { assertType } from 'src/app/common/util/assert';
import { UserService } from '../user.service';

/**
 * User-Dictionary service stores and retrieves key-value pairs
 * If the user is logged in, the saved key-value pairs are persistent across sessions and
 * only accessible to the user that created it.
 *
 * @author Albert Liu
 */
export type JSONValue = string|object|string[]|object[];

export type UserDictionary = {
  [Key: string]: JSONValue;
};

export enum EVENT_TYPE {
  GET,
  SET,
  DELETE,
  GET_ALL
}

export type GET_EVENT = {
  type: EVENT_TYPE.GET
  key: string
  value: JSONValue
};

export type SET_EVENT = {
  type: EVENT_TYPE.SET
  key: string
  value: JSONValue
};

export type DELETE_EVENT = {
  type: EVENT_TYPE.DELETE
  key: string
};

export type GET_ALL_EVENT = {
  type: EVENT_TYPE.GET_ALL
  value: UserDictionary
};

export type USER_DICT_EVENT = GET_EVENT | SET_EVENT | DELETE_EVENT | GET_ALL_EVENT;

export class NotReadyError extends Error {
  constructor (message?: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'NotReadyError';
  }
}

@Injectable({
  providedIn: 'root'
})
export class DictionaryService {
  public static readonly USER_DICTIONARY_ENDPOINT = 'users/dictionary';

  private dictionaryEventSubject = new Subject<USER_DICT_EVENT>();
  private localUserDictionary: UserDictionary = {}; // asynchronously initialized after construction (see initLocalDict)
  private ready: {promise: Promise<boolean>, value: boolean} = {promise: Promise.resolve(false), value: false};

  constructor(private http: HttpClient) {
    this.initLocalDict();
    this.handleDictionaryEventStream();
  }

  public getUserDictionary(): UserDictionary {
    if (this.ready.value === false) { throw new NotReadyError('incomplete initialization of user-dictionary service'); }
    return this.proxyUserDictionary(this.localUserDictionary);
  }

  public forceGetUserDictionary(): UserDictionary {
    // gets userdictionary even if local dictionary isn't initialized
    return this.proxyUserDictionary(this.localUserDictionary);
  }

  public getUserDictionaryAsync(): Promise<UserDictionary> {
    return this.ready.promise.then(() => this.getUserDictionary());
  }

  public getDictionaryEventStream(): Observable<USER_DICT_EVENT> {
    return this.dictionaryEventSubject.asObservable();
  }

  public get(key: string): Promise<string|object> {
    return this.http.get<string>(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}?key=${key}`)
    .toPromise()
    .then<string|object>(
      result => {
        try {
          result = JSON.parse(result);
          this.dictionaryEventSubject.next({type: EVENT_TYPE.GET, key: key, value: result});
          return result;
        } catch (e) {
          if (e instanceof SyntaxError) { // result was not json
            this.dictionaryEventSubject.next({type: EVENT_TYPE.GET, key: key, value: result});
            return result;
          } else {
            throw e;
          }
        }
      },
      reason => {
        assertType<HttpResponse<object>>(reason);
        switch (reason.status) {
          case 401:
            this.dictionaryEventSubject.next({type: EVENT_TYPE.GET, key: key, value: undefined as any});
            return undefined as any;
          default:
            this.dictionaryEventSubject.next({type: EVENT_TYPE.GET, key: key, value: undefined as any});
            console.warn(reason);
            return undefined as any;
        }
      }
    );
  }

  public set(key: string, value: string|object): Promise<boolean> {
    const strValue: String = (value instanceof String ? value : JSON.stringify(value));

    return this.http.post<string>(
      `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}?key=${key}`,
      strValue,
    )
    .toPromise()
    .then(
      () => {
        this.dictionaryEventSubject.next({type: EVENT_TYPE.SET, key: key, value: value});
        return true;
      },
      reason => {
        assertType<HttpResponse<object>>(reason);
        switch (reason.status) {
          case 401:
            this.dictionaryEventSubject.next({type: EVENT_TYPE.SET, key: key, value: value});
            return true;
          default:
            throw reason;
        }
      }
    );
  }

  public delete(key: string): Promise<boolean> {
    return this.http.delete<string>(
      `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}?key=${key}`
    )
    .toPromise()
    .then(
      () => {
        this.dictionaryEventSubject.next({type: EVENT_TYPE.DELETE, key: key});
        return true;
      },
      reason => {
        assertType<HttpResponse<object>>(reason);
        switch (reason.status) {
          case 401:
            this.dictionaryEventSubject.next({type: EVENT_TYPE.DELETE, key: key});
            return true;
          default:
            throw reason;
        }
      }
    );
  }

  public getAll(): Promise<Readonly<UserDictionary>> {
    return this.http.get(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`, {observe: 'response'})
    .toPromise()
    .then<UserDictionary>(
      result => {
        assertType<UserDictionary>(result.body);
        const value = result.body;
        this.dictionaryEventSubject.next({type: EVENT_TYPE.GET_ALL, value: value});
        return value;
      },
      reason => {
        assertType<HttpResponse<object>>(reason);
        switch (reason.status) {
          case 401:
            this.dictionaryEventSubject.next({type: EVENT_TYPE.GET_ALL, value: {}});
            return {} as any;
          default:
            this.dictionaryEventSubject.next({type: EVENT_TYPE.GET_ALL, value: {}});
            console.warn(reason);
            return {} as any;
        }
      }
    );
  }

  private initLocalDict() {
    let resolveReady: (read: boolean) => void;
    this.ready = { promise: new Promise((resolvefunc) => resolveReady = resolvefunc), value: false };

    // getAll automatically creates a dictionaryEvent that updates the localdict with the remote
    this.getAll().then(() => { this.ready.value = true; resolveReady(true); });

  }

  private handleDictionaryEventStream() {
    this.getDictionaryEventStream().subscribe(event => {
      switch (event.type) {
        case EVENT_TYPE.GET:
          if (event.key in this.localUserDictionary &&
            JSON.stringify(this.localUserDictionary[event.key]) !== JSON.stringify(event.value)) {
            console.warn(`[user-dictionary service] Dictionary desynchronized at key "${event.key}": locally had ${this.localUserDictionary[event.key]} but remote reported  ${event.value}`);
          }
          this.localUserDictionary[event.key] = event.value;
          break;

        case EVENT_TYPE.SET:
          this.localUserDictionary[event.key] = event.value;
          break;

        case EVENT_TYPE.DELETE:
          delete this.localUserDictionary[event.key];
          break;

        case EVENT_TYPE.GET_ALL:
          if (JSON.stringify(this.localUserDictionary) !== JSON.stringify(event.value)) {
            console.warn(`[user-dictionary service] Dictionary was desynchronized, local had ${this.localUserDictionary}, but remote reported ${event.value}`);

            // setting this.localUserDictionary = event.value would
            // ruin the references to this.localUserDictionary in all the proxy dictionaries
            for (const key in this.localUserDictionary) {
              if (this.localUserDictionary.hasOwnProperty(key)) {
                delete this.localUserDictionary[key];
              }
            }
            Object.assign(this.localUserDictionary, event.value);
          }
      }
    });
  }

  private proxyUserDictionary(snapshot: Readonly<UserDictionary>, ): UserDictionary {
    return new Proxy<UserDictionary>(snapshot, this.generateProxyHandler());
  }

  private generateProxyHandler(): ProxyHandler<UserDictionary> {
    const _this = this;
    return {
      set(localUserDictionary: Readonly<UserDictionary>, key: string, value: JSONValue) {
        _this.set(key, value);
        return true;
      },
      deleteProperty: function(localUserDictionary: Readonly<UserDictionary>, key: string) {
        _this.delete(key);
        return true;
      },
      defineProperty(localUserDictionary: Readonly<UserDictionary>, key: string, value: JSONValue) {
        _this.set(key, value);
        return true;
      }
    };
  }
}
