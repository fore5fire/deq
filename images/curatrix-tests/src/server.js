import path from 'path';
import fs from 'fs';
import moment from 'moment';
import { MongoClient } from 'mongodb';
import pino from 'pino';
import fetch from 'node-fetch';

const {
  MONGODB_TEST_ENDPOINT,
  TEST_TARGET_ENDPOINT,
  LOG_LEVEL,
  BOOTSTRAP_ADMIN_EMAIL,
  BOOTSTRAP_ADMIN_PASSWORD,
  JWT_PRIVATE_KEY_PATH
} = process.env;

// let db;
// before(async function () {
//   db = await MongoClient.connect(MONGODB_TEST_ENDPOINT);
//   await db.dropDatabase();
// });


class Server {

  constructor({ endpoint, bootstrapAdmin }) {
    this.endpoint = endpoint;
    this.bootstrapAdmin = bootstrapAdmin;
  }

  async request({ query, variables, headers = {}, method = 'POST', operationName }) {

    if (!query) {
      throw new Error("graphql-service-test-harness - #request: argument 'query' is required");
    }

    const result = await fetch(this.endpoint, {
      method: method,
      body: JSON.stringify({ query, variables, operationName }),
      headers: { 'Content-Type': 'application/json', ...headers }
    });

    const json = await result.json();

    if (json.errors) {
      throw json.errors;
    }
    return json.data;
  }

  async createUserAccount({ input, tokenExpiration = moment().add(1, 'day').format() }) {
    const { authGrant } = await this.request({
      query: `
        mutation CreateUserAccount($input: UserAccountInput!, $expiration: DateTime!) {
          authGrant: createUserAccount(input: $input) {
            refreshToken(expiration: $expiration)
            queryToken
            account {
              id
              ...on UserAccount {
                names {
                  first
                  last
                }
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
            ...on UserAccount {
              names {
                first
                last
              }
              email
            }
          }
        }
      `,
      variables: { id }
    });

    return account;
  }

  async getAccounts({ queryToken, filter }) {
    const { accounts } = await this.request({
      headers: { Authorization: `Bearer ${queryToken}` },
      query: `
        query GetAccounts($filter: AccountFilter) {
          accounts(filter: $filter) {
            id
            name
          }
        }
      `,
      variables: { filter }
    });

    return accounts;
  }

  async getUserAccounts({ queryToken, filter }) {
    const { userAccounts } = await this.request({
      headers: { Authorization: `Bearer ${queryToken}` },
      query: `
        query GetUserAccounts($filter: UserAccountFilter) {
          userAccounts(filter: $filter) {
            id
            names {
              first
              last
            }
            email
          }
        }
      `,
      variables: { filter }
    });

    return userAccounts;
  }

  async createUserToken({ email, password }) {
    const { createUserToken } = await this.request({
      query: `
        mutation CreateToken($expiration: DateTime!, $email: EmailAddress!, $password: String!) {
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
        mutation CreateToken($refreshToken: String!) {
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
        mutation CreateServiceToken($input: ServiceTokenInput!, $expiration: DateTime!) {
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

  async createPasswordResetToken({ queryToken, email }) {
    const { token } = await this.request({
      headers: { Authorization: `Bearer ${queryToken}` },
      query: `
        mutation CreatePasswordResetToken($email: EmailAddress!) {
          token: createPasswordResetToken(email: $email) {
            resetToken
            expiration
          }
        }
      `,
      variables: { email }
    });
    return token;
  }

  async changePassword({ resetToken, email, newPassword }) {
    const { authGrant } = await this.request({
      query: `
        mutation ChangePassword($email: EmailAddress!, $newPassword: String!, $resetToken: String!) {
          authGrant: changePassword(email: $email, resetToken: $resetToken, newPassword: $newPassword) {
            queryToken
            refreshToken
            account {
              id
            }
          }
        }
      `,
      variables: { email, newPassword, resetToken }
    });
    return authGrant;
  }
};

global.publicKey = fs.readFileSync(path.resolve(__dirname, '../test-keys/public.pem'));


global.Server = new Server({
  endpoint: TEST_TARGET_ENDPOINT,
  bootstrapAdmin: {
    email: BOOTSTRAP_ADMIN_EMAIL,
    password: BOOTSTRAP_ADMIN_PASSWORD,
  },
  jwt: {
    privateKey: "",
    publicKey: "",
  }
});
