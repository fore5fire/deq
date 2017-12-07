
import { makeExecutableSchema } from 'graphql-tools';
import typeDefs from './schema.graphql';
import { Account, UserAccount } from './account';
import { RefreshToken } from './refresh-token';
import { GraphQLDateTime } from 'graphql-iso-date';
import { GraphQLEmailAddress } from 'graphql-scalars';
import { GraphQLString } from 'graphql/type';

const resolvers = {
  Query: {
    account(obj, { id }, context) {
      console.log(context);
      if (!id) {
        return Account.findById(context.user?.aid);
      }

      if (id !== context.user?.aid && !(context.user?.pems?.auth?.viewAcct)) {
        throw new Error("Access Denied");
      }

      return Account.findById(id);
    },
  },
  Mutation: {
    async createUserAccount(obj, { input }, { secretKeyPath }) {
      try {
        const account = new UserAccount(input);
        await account.save();
        log.debug("createUserAccount", { input, account });
        return account.authGrant({ secretKeyPath });
      }
      catch (error) {
        if (error.name === 'MongoError' && error.code == 11000) {
          log.debug("attempted to register user with duplicate email address", { error } );
          throw new Error("Email is already in use");
        }
        log.warn("Error creating user account", { error });
        throw error;
      }
    },
    account(obj, { id }, context) {

      if (!id) {
        return Account.findById(context.user?.aid);
      }

      if (id !== context.user?.aid && !(context.user?.pems?.auth?.editAcct)) {
        throw new Error("Permission Denied");
      }

      return Account.findById(id);
    },

    async createUserToken(obj, { email, password }, { secretKeyPath }) {
      log.debug('CreateUserToken');
      const account = await UserAccount.findOne({ email });
      log.debug({ account });
      if (!account || !await account.authenticate(password)) {
        throw new Error("Invalid credentials");
      }
      log.debug('Getting Auth Grant');
      return account.authGrant({ secretKeyPath });
    },

    async createRefreshedToken(obj, { refreshToken }, { secretKeyPath }) {
      const token = await RefreshToken.findOne({ 'refreshToken.value': refreshToken });
      if (!token) {
        throw new Error("Invalid credentials");
      }
      return token.refresh({ secretKeyPath });
    }
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
  Email: GraphQLEmailAddress,
  QueryToken: GraphQLString,
  RefreshToken: GraphQLString,
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
