
import { makeExecutableSchema } from 'graphql-tools';
import typeDefs from './schema.graphql';
import Greeting from './greeting';

const resolvers = {
  Query: {
    greeting(obj, args, context, info) {
      return new Greeting();
    }
  },
  // Mutation: {
  //
  // }
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
