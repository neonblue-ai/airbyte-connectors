import {AirbyteConfig} from 'faros-airbyte-cdk';

export interface KlaviyoConfig extends AirbyteConfig {
  readonly credentials:
    | {
        auth_type: 'oauth';
        client_id: string;
        client_secret: string;
        refresh_token: string;
      }
    | {auth_type: 'api_key'; api_key: string};
  readonly intialize?: boolean;
}

// https://gist.github.com/kuroski/9a7ae8e5e5c9e22985364d1ddbf3389d
type CamelToSnakeCase<S extends string> = S extends `${infer T}${infer U}`
  ? `${T extends Capitalize<T> ? '_' : ''}${Lowercase<T>}${CamelToSnakeCase<U>}`
  : S;
export type KeysToSnakeCase<T> = {
  [K in keyof T as CamelToSnakeCase<string & K>]: T[K];
};

export type DatePropertiesToString<T> = {
  [K in keyof T]: T[K] extends Date ? string : T[K];
};
