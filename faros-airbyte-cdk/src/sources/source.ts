import {AirbyteConnector} from '../connector';
import {
  AirbyteCatalogMessage,
  AirbyteConfig,
  AirbyteConfiguredCatalog,
  AirbyteMessage,
  AirbyteState,
} from '../protocol';

/**
 * Airbyte Source
 * https://docs.airbyte.io/understanding-airbyte/airbyte-specification#source
 */
export abstract class AirbyteSource<
  Config extends AirbyteConfig,
> extends AirbyteConnector {
  /**
   * @returns An AirbyteCatalog representing the available streams and fields in
   * this integration. For example, given valid credentials to a Postgres
   * database, returns an Airbyte catalog where each postgres table is a stream,
   * and each table column is a field.
   */
  abstract discover(config: Config): Promise<AirbyteCatalogMessage>;

  /**
   * Override this method to update the config before running the connector.
   * For example - you might want to run a filter on a list of strings, one
   * which would require an async call. In that case, this method should be
   * added to your base class, e.g. the "{AirbyteName}Source" class, for an
   * example look at the CircleCISource.
   */
  async onBeforeRead(
    config: Config,
    catalog: AirbyteConfiguredCatalog,
    state?: AirbyteState
  ): Promise<{
    config: Config;
    catalog: AirbyteConfiguredCatalog;
    state?: AirbyteState;
  }> {
    return {config, catalog, state};
  }

  /**
   * @returns A generator of the AirbyteMessages generated by reading the source
   * with the given configuration, catalog, and state.
   */
  abstract read(
    config: Config,
    redactedConfig: AirbyteConfig,
    catalog: AirbyteConfiguredCatalog,
    state: AirbyteState
  ): AsyncGenerator<AirbyteMessage>;
}
