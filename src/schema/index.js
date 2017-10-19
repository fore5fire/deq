
import { makeExecutableSchema } from 'graphql-tools';
import typeDefs from './schema.graphql';
import { Task, MutableTask, MutableAssetTask } from './task';
import { GraphQLDate } from 'graphql-iso-date';
import { GraphQLUrl } from 'graphql-url';

const resolvers = {
  Query: {
    async task(obj, { id }) {
      return await Task.findById(id);
    },
    async tasks(obj, { filter }) {
      return await Task.find(filter).exec();
    }
  },
  Mutation: {
    async createAssetTask(obj, { input }) {
      const task = new MutableAssetTask(input);
      await task.save();
      return task;
    },
    async task(obj, { id }) {
      return await MutableTask.findById(id);
    }
  },
  Task: {
    __resolveType(obj, context, info) {
      return info.schema.getType(obj.type);
    }
  },
  MutableTask: {
    __resolveType(obj, context, info) {
      return info.schema.getType(obj.type);
    }
  },
  Date: GraphQLDate,
  Url: GraphQLUrl
};


export default makeExecutableSchema({
  typeDefs,
  resolvers,
  logger: { log: e => console.log(e) }, // optional
  resolverValidationOptions: {
    requireResolversForNonScalar: false,
    requireResolversForArgs: false
  }, // optional
});
