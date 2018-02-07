
import { makeExecutableSchema } from 'graphql-tools';
import typeDefs from './schema.graphql';
import { Account, UserAccount } from './account';
import { RefreshToken } from './refresh-token';
import { GraphQLDateTime } from 'graphql-iso-date';
import { GraphQLEmailAddress } from 'graphql-scalars';
import { AuthGrant } from './auth-grant';
import { ValidationError } from 'graphql-error-codes';
import GraphQLJSON from 'graphql-type-json';
import moment from 'moment';
import fs from 'fs-extra';
import jwt from 'jsonwebtoken';
import traverse from 'traverse';
// import escapeStringRegexp from 'escape-string-regexp';

const resolvers = {
  Query: {
    async account(obj, { id }, { user, userToken }) {


      if (!id) {
        await user.mustBe('logged in to account');
        return Account.findById(userToken?.aid);
      }

      await user.mustBeAbleTo('view user account', id);

      return Account.findById(id);
    },
    async accounts(obj, { filter = {} }, { user }) {

      await user.mustBeAbleTo('view user accounts');
      // await user.mustBeAbleTo('view service account');

      const args = {
        ...filter,
        cursor: { limit: 10, reversed: false, ...filter.cursor },
      };

      const { cursor, name, hasPermissions } = args;

      if (cursor.limit <= 0) {
        throw new ValidationError("out of range - must be positive", "cursor.limit");
      }

      // const escapedName = escapeStringRegexp(name);

      const query = {
        // name: { '$regex': new RegExp(`^${escapedName}` }
      };

      if (hasPermissions) {
        const permissions = hasPermissions.reduce((pems, current) => pems[current.domain] = current.value, {});
        const traversed = traverse(permissions);
        traversed.paths().reduce((result, path) => {
          result[path] = `_permissions.${traversed.get(path)}`;
        }, query);
      }

      if (cursor.reversed) {
        query._id = { '$lt': cursor.start };
      }
      else {
        query._id = { '$gt': cursor.start };
      }

      return Account.find(query).limit(cursor.limit).exec();
    },
    async userAccounts(obj, { filter = {} }, { user, userToken }) {

      await user.mustBeAbleTo('view user accounts');

      const args = {
        ...filter,
        cursor: { limit: 10, reversed: false, ...filter.cursor },
      };

      const { cursor, names, hasPermissions } = args;

      if (cursor.limit <= 0) {
        throw new ValidationError("out of range - must be positive", "cursor.limit");
      }

      const query = {
        type: 'UserAccount',
      };

      if (hasPermissions) {
        const permissions = hasPermissions.reduce((pems, current) => pems[current.domain] = current.value, {});
        const traversed = traverse(permissions);
        traversed.paths().reduce((result, path) => {
          result[`_permissions.${path}`] = traversed.get(path);
        }, query);
      }

      if (cursor.reversed) {
        query._id = { $lt: cursor.start };
      }
      else {
        query._id = { $gt: cursor.start };
      }

      return Account.find(query).limit(cursor.limit).exec();
    },

  },
  Mutation: {
    async createUserAccount(obj, { input }, { secretKeyPath, user }) {
      await user.mustBeAbleTo('create user account');

      input.email = input.email.toLowerCase();

      try {
        const account = new UserAccount(input);
        await account.save();
        log.debug("createUserAccount", { input, account });
        return new AuthGrant({ account, pems: account._permissions, validFor: '1h', secretKeyPath });
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
    async account(obj, { id }, { user, userToken }) {

      if (!id) {
        await user.mustBe('logged in to account');
        return Account.findById(userToken?.aid);
      }

      await user.mustBeAbleTo('edit user account', id);

      return Account.findById(id);
    },

    async createUserToken(obj, { email, password }, { secretKeyPath, user }) {

      const account = await UserAccount.findOne({ email: email.toLowerCase() });

      await user.mustBeAbleTo('create user token', account, password);

      return new AuthGrant({ account, pems: account._permissions, validFor: '1h', secretKeyPath });
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

      const { permissions } = input;

      await user.mustBeAbleTo('create service token', { domains: permissions.map(pem => pem.domain) });

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
      console.log(token);
      await user.mustBeAbleTo('refresh token', token);

      // if (await user.isnt('within token refresh period', token?.queryToken.validFor)) {
      //   log.warn("Client attempted to refresh token earlier than grace period before expiration! Possible sign of unathorized access to refresh token");
      //   throw new AuthError("To early to refresh token! It's possible someone else is using your refresh token - consider revoking it.");
      // }

      return token.refresh({ secretKeyPath });
    },

    async updateUserToken(obj, args, { user, userToken, secretKeyPath }) {

      if (!userToken?.aid) {
        return;
      }

      const account = await UserAccount.findById(userToken.aid);
      await user.mustBeAbleTo('update user token', account);

      const secretKeyPromise = fs.readFile(secretKeyPath);

      await RefreshToken.updateMany({
        'queryToken.payload.aid': userToken.aid
      },
      {
        $set: { 'queryToken.payload.pems': account._permissions }
      });
      // await Promise.all(
      //   tokens.map(token => {
      //     token.queryToken.payload.pems = account._permissions;
      //     return token.save();
      //   })
      // );

      const secretKey = await secretKeyPromise;
      return jwt.sign({ aid: userToken.aid, exp: userToken.exp, pems: account._permissions }, secretKey, { algorithm: 'ES384' });
    },

    async changePassword(obj, { email, newPassword, resetToken }, { user, secretKeyPath }) {

      const token = jwt.verify(resetToken, await fs.readFile(secretKeyPath));

      const account = await UserAccount.findOne({ email: email.toLowerCase() });

      await user.mustBeAbleTo('reset user password', account.id, token);

      account.password = newPassword;
      await account.save();

      return new AuthGrant({ account, pems: account._permissions, validFor: '1h', secretKeyPath });
    },

    async createPasswordResetToken(obj, { email }, { user, secretKeyPath }) {

      await user.mustBeAbleTo('create user password reset token');
      const account = await Account.findOne({ email: email.toLowerCase() });

      if (!account) {
        return;
      }

      const payload = { pems: { auth: { acct: { usr: { resetPass: account._id } } } } };

      return {
        resetToken: jwt.sign(payload, await fs.readFile(secretKeyPath), { expiresIn: '1h' }),
        expiration: moment().add(1, 'hour').toDate(),
      };
    },
  },
  Account: {
    __resolveType(obj) {
      log.debug('AccountType');
      return obj.type;
    }
  },
  MutableAccount: {
    __resolveType(obj) {
      log.debug('MutableAccountType');
      return `Mutable${obj.type}`;
    }
  },

  DateTime: GraphQLDateTime,
  EmailAddress: GraphQLEmailAddress,
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
