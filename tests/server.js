import { Harness } from 'graphql-service-test-harness';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import moment from 'moment';
import portfinder from 'portfinder';
import pickBy from 'lodash.pickby';

global.Server = async args => {
  const port = await portfinder.getPortPromise();
  portfinder.basePort = port + 1;
  const healthPort = await portfinder.getPortPromise();

  return new Server({ port, healthPort, ...args });
};


class Server extends Harness {

  constructor({ port, healthPort, mongodbEndpoint, bootstrapAdminEmail, bootstrapAdminPassword }) {
    super({
      endpoint: `http://localhost:${port}/graphql`,
      healthEndpoint: `http://localhost:${healthPort}/ready`,

      async start() {

        return spawn('npm', ['start'], {
          stdio: 'inherit',
          env: pickBy({
            ...process.env,
            PORT: port.toString(),
            HEALTH_PORT: healthPort.toString(),
            MONGODB_ENDPOINT: mongodbEndpoint,
            LOG_LEVEL: 'warn',
            JWT_PRIVATE_KEY_PATH: path.resolve(__dirname, 'test-keys/private.pem'),
            BOOTSTRAP_ADMIN_EMAIL: bootstrapAdminEmail,
            BOOTSTRAP_ADMIN_PASSWORD: bootstrapAdminPassword,
          }, x => x)
        });
      },

      stop(subprocess) {
        const ret = new Promise(accept => {
          subprocess?.on('exit', function () {
            accept();
          });
        });
        subprocess?.kill();
        return ret;
      }
    });

    this.mongodbEndpoint = mongodbEndpoint;
  }

  async createUserAccount({ input, tokenExpiration = moment().add(1, 'day').format() }) {
    const { authGrant } = await this.request({
      query: `
        mutation CreateUserAccount($input: UserAccountInput!, $expiration: Date!) {
          authGrant: createUserAccount(input: $input) {
            refreshToken(expiration: $expiration)
            queryToken
            account {
              id
              name
              ...on UserAccount {
                email
              }
            }
          }
        }
      `,
      variables: { input, expiration: tokenExpiration },
    });

    return authGrant;
  }

  async getAccount({ queryToken, id }) {
    const { account } = await this.request({
      headers: { Authorization: `Bearer ${queryToken}` },
      query: `
        query GetAccount($id: ID) {
          account(id: $id) {
            id
            name
            ...on UserAccount {
              email
            }
          }
        }
      `,
      variables: { id }
    });

    return account;
  }

  async createUserToken({ email, password }) {
    const { createUserToken } = await this.request({
      query: `
        mutation CreateToken($expiration: Date!, $email: EmailAddress!, $password: String!) {
          createUserToken(email: $email, password: $password) {
            queryToken
            refreshToken(expiration: $expiration)
            account {
              id
              name
            }
          }
        }
      `,
      variables: { email, password, expiration: moment().add(1, 'hour').format() }
    });
    return createUserToken;
  }

  async createRefreshedToken({ refreshToken }) {
    const { queryToken } = await this.request({
      query: `
        mutation CreateToken($refreshToken: RefreshToken!) {
          queryToken: createRefreshedToken(refreshToken: $refreshToken)
        }
      `,
      variables: { refreshToken }
    });
    return queryToken;
  }

  async createServiceToken({ queryToken, input }) {
    const { authGrant } = await this.request({
      headers: { Authorization: `Bearer ${queryToken}` },
      query: `
        mutation CreateServiceToken($input: ServiceTokenInput!, $expiration: Date!) {
          authGrant: createServiceToken(input: $input) {
            queryToken
            refreshToken(expiration: $expiration)
          }
        }
      `,
      variables: { input, expiration: moment().add(1, 'hour').format() }
    });
    return authGrant;
  }
};

global.publicKey = fs.readFileSync(path.resolve(__dirname, 'test-keys/public.pem'));
