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
