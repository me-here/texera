export interface Source {
  type: string;
  id: string;
}

export interface Metadata {
  primary: boolean;
  source: Source;
}

export interface Photo {
  metadata: Metadata;
  url: string;
}

export interface GooglePeopleApiResponse {
  resourceName: string;
  etag: string;
  photos: Photo[];
}
