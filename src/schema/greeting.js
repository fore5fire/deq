

export default class Greeting {
  say = 'hello world';
  exclaim({ name }) {
    return `Hello ${name}!!!`;
  }
  _private = 'This is a private variable because it\'s not in the schema';
}
