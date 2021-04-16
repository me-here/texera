import { HttpClient } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed, inject, fakeAsync, tick, flushMicrotasks, ComponentFixtureAutoDetect, flush } from '@angular/core/testing';
import { Subject } from 'rxjs';
import { AppSettings } from 'src/app/common/app-setting';
import { DictionaryService, EVENT_TYPE, NotReadyError, UserDictionary, USER_DICT_EVENT } from './dictionary.service';

describe('DictionaryService', () => {
  let dictionaryService: DictionaryService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        DictionaryService,
      ],
      imports: [
        HttpClientTestingModule,
      ]
    });

    dictionaryService = TestBed.inject(DictionaryService);
  });

  it('should be created', inject([DictionaryService], (injectedService: DictionaryService) => {
    expect(injectedService).toBeTruthy();
  }));

  it('should throw exceptions if accessed before initialization', () => {
    // make sure getAll, and initialization, never finishes
    spyOn(DictionaryService.prototype, 'getAll').and.returnValue(new Promise(() => {}));
    dictionaryService = new DictionaryService(TestBed.inject(HttpClient));
    expect(() => dictionaryService.getUserDictionary()).toThrowError(NotReadyError);
  });

  it('should return an empty dictionary if force accessed before initialization', () => {
    // make sure getAll, and initialization, never finishes
    spyOn(DictionaryService.prototype, 'getAll').and.returnValue(new Promise(() => {}));
    dictionaryService = new DictionaryService(TestBed.inject(HttpClient));
    const dictionary = dictionaryService.forceGetUserDictionary();
    expect((Object.keys(dictionary).length === 0)).toBeTrue();
  });

  it('should only resolve dictionary promise once initialized', fakeAsync(() => {
    let promiseCompleted = false;
    let resolvePromise: (value: UserDictionary) => void = () => {};
    spyOn(DictionaryService.prototype, 'getAll').and.returnValue(new Promise((resolve) => {
      resolvePromise = resolve;
    }));
    dictionaryService = new DictionaryService(TestBed.inject(HttpClient));
    const dictionaryPromise = dictionaryService.getUserDictionaryAsync();
    dictionaryPromise.then(() => promiseCompleted = true);
    tick();
    expect(promiseCompleted).toBeFalse();
    resolvePromise({});
    tick();
    expect(promiseCompleted).toBeTrue();
  }));

  it('should initialize the local dictionary properly', fakeAsync(() => {
    const httpMock = TestBed.inject(HttpTestingController);
    const testDict = { a: 'a', b: 'b', c: 'c' };

    dictionaryService = new DictionaryService(TestBed.inject(HttpClient));
    const mockReqs = httpMock.match(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`);
    expect(mockReqs.length).toEqual(2); // previous request is from previous initialization of dictionary service in beforeEach
    const mockReq = mockReqs[1];
    expect(mockReq.cancelled).toBeFalsy();
    expect(mockReq.request.responseType).toEqual('json');
    mockReq.flush(testDict);
    tick();

    let dict = dictionaryService.getUserDictionary();
    expect(dict).toEqual(testDict);
    dict = dictionaryService.forceGetUserDictionary();
    expect(dict).toEqual(testDict);
    dictionaryService.getUserDictionaryAsync().then(value => dict = value);
    tick();
    expect(dict).toEqual(testDict);
  }));

  it('should produce http requests for all get/set/delete operations', fakeAsync(() => {
    const testDict = { a: 'a', b: 'b', c: 'c' };
    const httpMock = TestBed.inject(HttpTestingController);
    const apiEndpoint = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;
    // first request is from initialization of dictionary service in beforeEach
    let req = httpMock.expectOne(`${apiEndpoint}`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('GET');
    expect(req.request.responseType).toEqual('json');
    req.flush(testDict);

    dictionaryService.getAll();
    req = httpMock.expectOne(`${apiEndpoint}`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('GET');
    expect(req.request.responseType).toEqual('json');
    req.flush(testDict);

    dictionaryService.get('test');
    req = httpMock.expectOne(`${apiEndpoint}?key=test`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('GET');
    expect(req.request.responseType).toEqual('json');
    req.flush(`${JSON.stringify('testValue')}`);

    dictionaryService.set('test', 'testValue');
    req = httpMock.expectOne(`${apiEndpoint}?key=test`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('POST');
    expect(req.request.body).toEqual(`${JSON.stringify('testValue')}`);
    expect(req.request.responseType).toEqual('json');
    req.flush('');

    dictionaryService.delete('test');
    req = httpMock.expectOne(`${apiEndpoint}?key=test`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('DELETE');
    expect(req.request.body).toEqual(null);
    expect(req.request.responseType).toEqual('json');
    req.flush('');

    flush();

    httpMock.verify();
  }));

  it('should emit events for all get/set/delete operations', fakeAsync(() => {
    const testDict = { a: 'a', b: 'b', c: 'c' };
    const httpMock = TestBed.inject(HttpTestingController);
    const apiEndpoint = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;

    const subjectSpy = spyOn((dictionaryService as any).dictionaryEventSubject, 'next');

    // first request is from initialization of dictionary service in beforeEach
    let req = httpMock.expectOne(`${apiEndpoint}`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('GET');
    expect(req.request.responseType).toEqual('json');
    req.flush(testDict);
    tick();
    expect(subjectSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.GET_ALL, value: testDict});
    subjectSpy.calls.reset();

    dictionaryService.getAll();
    req = httpMock.expectOne(`${apiEndpoint}`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('GET');
    expect(req.request.responseType).toEqual('json');
    req.flush(testDict);
    tick();
    expect(subjectSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.GET_ALL, value: testDict});
    subjectSpy.calls.reset();

    dictionaryService.get('test');
    req = httpMock.expectOne(`${apiEndpoint}?key=test`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('GET');
    expect(req.request.responseType).toEqual('json');
    req.flush(`${JSON.stringify('testValue')}`);
    tick();
    expect(subjectSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.GET, key: 'test', value: 'testValue'});
    subjectSpy.calls.reset();

    dictionaryService.set('test', 'testValue');
    req = httpMock.expectOne(`${apiEndpoint}?key=test`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('POST');
    expect(req.request.body).toEqual(`${JSON.stringify('testValue')}`);
    expect(req.request.responseType).toEqual('json');
    req.flush('');
    tick();
    expect(subjectSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.SET, key: 'test', value: 'testValue'});
    subjectSpy.calls.reset();

    dictionaryService.delete('test');
    req = httpMock.expectOne(`${apiEndpoint}?key=test`);
    expect(req.cancelled).toBeFalsy();
    expect(req.request.method).toEqual('DELETE');
    expect(req.request.body).toEqual(null);
    expect(req.request.responseType).toEqual('json');
    req.flush('');
    tick();
    expect(subjectSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.DELETE, key: 'test'});
    subjectSpy.calls.reset();

    flush();

    httpMock.verify();
  }));

  it('should handle dictionary events by updating the local dictionary', fakeAsync(() => {
    const testDict = { a: 'a', b: 'b', c: 'c' };
    const localdict = (dictionaryService as any).localUserDictionary;
    const subject: Subject<USER_DICT_EVENT> = (dictionaryService as any).dictionaryEventSubject;

    subject.next({type: EVENT_TYPE.GET_ALL, value: testDict});
    tick();
    expect(localdict).toEqual(testDict);

    subject.next({type: EVENT_TYPE.GET, key: 'testKey', value: 'testValue'});
    tick();
    expect(localdict.testKey).toEqual('testValue');

    subject.next({type: EVENT_TYPE.SET, key: 'testKey2', value: 'testValue2'});
    tick();
    expect(localdict.testKey2).toEqual('testValue2');

    subject.next({type: EVENT_TYPE.DELETE, key: 'testKey2'});
    tick();
    expect(localdict.testKey2).toBeUndefined();
  }));

  it('should keep generated dicts in sync with localdict', fakeAsync(() => {
    const testDict = { a: 'a', b: 'b', c: 'c' };
    const localdict = (dictionaryService as any).localUserDictionary;
    const generatedDict = dictionaryService.forceGetUserDictionary();
    const subject: Subject<USER_DICT_EVENT> = (dictionaryService as any).dictionaryEventSubject;

    subject.next({type: EVENT_TYPE.GET_ALL, value: testDict});
    tick();
    expect(localdict).toEqual(testDict);
    expect(generatedDict).toEqual(localdict);

    subject.next({type: EVENT_TYPE.GET, key: 'testKey', value: 'testValue'});
    tick();
    expect(localdict.testKey).toEqual('testValue');
    expect(generatedDict).toEqual(localdict);

    subject.next({type: EVENT_TYPE.SET, key: 'testKey2', value: 'testValue2'});
    tick();
    expect(localdict.testKey2).toEqual('testValue2');
    expect(generatedDict).toEqual(localdict);

    subject.next({type: EVENT_TYPE.DELETE, key: 'testKey2'});
    tick();
    expect(localdict.testKey2).toBeUndefined();
    expect(generatedDict).toEqual(localdict);
  }));

});
