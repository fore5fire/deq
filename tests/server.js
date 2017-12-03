import { Harness } from 'graphql-service-test-harness';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import moment from 'moment';

global.Server = class Server extends Harness {

  constructor({ port = 3000, healthPort = 4000, mongodbEndpoint }) {
    super({
      endpoint: `http://localhost:${port}/graphql`,
      healthEndpoint: `http://localhost:${healthPort}/ready`,

      async start() {
        return spawn('npm', ['start'], {
          stdio: 'inherit',
          env: {
            PORT: port,
            HEALTH_PORT: healthPort,
            MONGODB_ENDPOINT: mongodbEndpoint,
            // LOG_LEVEL: 'info',
            JWT_PRIVATE_KEY_PATH: path.resolve(__dirname, 'test-keys/private.pem'),
            ...process.env
          }
        });
      },

      async stop(subprocess) {
        subprocess.kill();
      }
    });

    this.mongodbEndpoint = mongodbEndpoint;
  }

  async createUserAccount(input) {
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
      variables: { input, expiration: moment().add(1, 'day').format() },
    });

    return authGrant;
  }

  async id({ queryToken, id }) {
    const { account } = await this.request({
      headers: { Authorization: `Bearer ${queryToken}` },
      query: `
        query GetId($id: ID) {
          account(id: $id) {
            id
          }
        }
      `,
      variables: { id }
    });

    return account.id;
  }

  async createUserToken({ email, password }) {
    const { createUserToken } = await this.request({
      headers: { Authorization: `Bearer ${this.queryToken}` },
      query: `
        mutation CreateToken($expiration: Date!, $email: Email!, $password: String!) {
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
      headers: { Authorization: `Bearer ${this.queryToken}` },
      query: `
        mutation CreateToken($refreshToken: RefreshToken!) {
          queryToken: createRefreshedToken(refreshToken: $refreshToken)
        }
      `,
      variables: { refreshToken }
    });
    return queryToken;
  }
};

global.publicKey = fs.readFileSync(path.resolve(__dirname, 'test-keys/public.pem'));
