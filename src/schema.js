
import { makeExecutableSchema } from 'graphql-tools';
import typeDefs from './schema.graphql';
import { Account, UserAccount } from './account';
import { RefreshToken } from './refresh-token';
import { GraphQLDateTime } from 'graphql-iso-date';
import { GraphQLEmailAddress } from 'graphql-scalars';
import { GraphQLString } from 'graphql/type';
import groupBy from 'lodash.groupby';
import { AuthGrant } from './auth-grant';
import { AuthError, ValidationError } from 'graphql-error-codes';
import GraphQLJSON from 'graphql-type-json';

const resolvers = {
  Query: {
    async account(obj, { id }, { user, aid }) {

      await user.mustBe('logged in to user account');

      if (!id) {
        return Account.findById(aid);
      }

      await user.mustBeAbleTo('view user account', id);

      return Account.findById(id);
    },
  },
  Mutation: {
    async createUserAccount(obj, { input }, { secretKeyPath, user }) {
      await user.mustBeAbleTo('create user account');

      try {
        const account = new UserAccount(input);
        await account.save();
        log.debug("createUserAccount", { input, account });
        return new AuthGrant({ account, pems: account.pems, validFor: '1h', secretKeyPath });
      }
      catch (error) {
        if (error.name === 'MongoError' && error.code == 11000) {
          log.debug("attempted to register user with duplicate email address", { error } );
          throw new ValidationError("Email is already in use", "email");
        }
        log.warn("Error creating user account", { error });
        throw error;
      }
    },
    async account(obj, { id }, { user, aid }) {

      await user.mustBe('logged in to user account');

      if (!id) {
        return Account.findById(aid);
      }

      await user.mustBeAbleTo('edit user account', id);

      return Account.findById(id);
    },

    async createUserToken(obj, { email, password }, { secretKeyPath, user }) {

      const account = await UserAccount.findOne({ email });

      await user.mustBeAbleTo('create user token', account, password);

      return new AuthGrant({ account, pems: account?._permissions, validFor: '1h', secretKeyPath });
    },

    async revokeUserToken(obj, { refreshToken }, { user }) {
      await user.mustBeAbleTo('revoke user token');

      try {
        await RefreshToken.remove({ 'refreshToken.value': refreshToken , type: 'user' }).exec();
        return true;
      }
      catch (error) {
        log.error({ msg: "Error revoking token", ...error });
        return false;
      }
    },

    async createServiceToken(obj, { input }, { secretKeyPath, user }) {
      await user.mustBeAbleTo('create service token');

      const pems = {};
      input.permissions.forEach(pem => {
        pems[pem.domain] = pem.value;
      });

      return new AuthGrant({ pems, validFor: '1h', secretKeyPath, type: 'service' });
    },

    async revokeServiceToken(obj, { refreshToken }, { user }) {
      await user.mustBeAbleTo('revoke service token');

      try {
        await RefreshToken.remove({ 'refreshToken.value': refreshToken, type: 'service' }).exec();
        return true;
      }
      catch (error) {
        log.error({ msg: "Error revoking token", ...error });
        return false;
      }
    },

    async createRefreshedToken(obj, { refreshToken }, { secretKeyPath, user }) {

      const token = await RefreshToken.findOne({ 'refreshToken.value': refreshToken });

      await user.mustBeAbleTo('refresh token', token);

      if (await user.isnt('within token refresh period', token?.refreshToken.expiration)) {
        log.warn("Client attempted to refresh token earlier than grace period before expiration! Possible sign of unathorized access to refresh token");
        throw new AuthError("To early to refresh token! It's possible someone else is using your refresh token - consider revoking it.");
      }

      return token.refresh({ secretKeyPath });
    },
  },
  Account: {
    __resolveType(obj) {
      return obj.type;
    }
  },
  MutableAccount: {
    __resolveType(obj) {
      return obj.type;
    }
  },
  Date: GraphQLDateTime,
  EmailAddress: GraphQLEmailAddress,
  QueryToken: GraphQLString,
  RefreshToken: GraphQLString,
  Json: GraphQLJSON,
};


export const schema = makeExecutableSchema({
  typeDefs,
  resolvers,
  logger: { log: e => console.log(e) }, // optional
  resolverValidationOptions: {
    requireResolversForNonScalar: false,
    requireResolversForArgs: false
  }, // optional
});

export async function ready() {
  await Promise.all([
    new Promise((...args) => UserAccount.on('index', handler(...args))),
    new Promise((...args) => RefreshToken.on('index', handler(...args))),
  ]);
  return schema;
}


function handler(accept, reject) {
  return error => {
    if (error) {
      reject(error);
    }
    else {
      accept();
    }
  };
};
