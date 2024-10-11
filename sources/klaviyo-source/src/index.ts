import {Command} from 'commander';
import {
  AirbyteSourceBase,
  AirbyteSourceBaseV2,
  AirbyteSourceLogger,
  AirbyteSourceRunner,
  AirbyteSpec,
  AirbyteStreamBase,
} from 'faros-airbyte-cdk';
import {ApiKeySession} from 'klaviyo-api';
import _ from 'lodash';
import VError from 'verror';

import {getKlaviyoOAuthSession, Klaviyo} from './klaviyo';
import * as Streams from './streams';
import {KlaviyoConfig} from './types';

function getKlaviyoClient(config: KlaviyoConfig) {
  if (config.credentials.auth_type === 'api_key') {
    return new Klaviyo(new ApiKeySession(config.credentials.api_key));
  } else if (config.credentials.auth_type === 'oauth') {
    return new Klaviyo(
      getKlaviyoOAuthSession({
        clientId: config.credentials.client_id,
        clientSecret: config.credentials.client_secret,
        refreshToken: config.credentials.refresh_token,
      })
    );
  }
  throw new Error(
    `Invalid auth type (${(config.credentials as any).auth_type}).`
  );
}

export function mainCommand(): Command {
  const logger = new AirbyteSourceLogger();
  const source = new KlaviyoSource(logger);
  return new AirbyteSourceRunner(logger, source).mainCommand();
}

export class KlaviyoSource extends AirbyteSourceBaseV2<KlaviyoConfig> {
  get type(): string {
    return 'klaviyo';
  }

  async spec(): Promise<AirbyteSpec> {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return new AirbyteSpec(require('../resources/spec.json'));
  }

  async checkConnection(config: KlaviyoConfig): Promise<[boolean, VError]> {
    try {
      const client = getKlaviyoClient(config);
      await client.accounts.getAccounts();
      return [true, undefined];
    } catch (error) {
      return [false, new VError(error, 'Connection check failed')];
    }
  }

  streams(config: KlaviyoConfig): AirbyteStreamBase[] {
    const client = getKlaviyoClient(config);
    return _.map(Streams, (S) => new S(this.logger, config, client));
  }
}
