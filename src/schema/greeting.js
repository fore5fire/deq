

export default class Greeting {

  constructor(name) {
    this.name = name;
  }

  say() {
    return `my name is ${this.name}. What's your name?`;
  }
  exclaim({ name }) {
    return `Hello ${name}!!!`;
  }
  response({ name }) {
    return new Greeting(name);
  }
  _private = 'This is a private variable because it\'s not in the schema';
}
