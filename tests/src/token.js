import chai, { expect } from 'chai';
import Mongo from 'mongo-in-memory';
import portfinder from 'portfinder';
import jwt from 'jsonwebtoken';
import chaiAsPromised from 'chai-as-promised';
import uuidv4 from 'uuid/v4';

chai.use(chaiAsPromised);

const exampleUser = {
  email: 'example@example.com',
  name: 'Example User',
  password: 'This is actually a secure password',
};
const exampleUser2 = {
  email: 'example2@gmail.com',
  name: 'Another User',
  password: 'This is a different secure password',
};
const exampleUser3 = {
  email: 'numba3@yahoo.com',
  name: 'Example User',
  password: 'This is a different secure password',
};


describe('Tokens', function () {

  before(async function () {
    this.timeout(10000);
    const port = await portfinder.getPortPromise();
    this.mongo = new Mongo(port);
    await this.mongo.start();
  });

  after(async function () {
    await this.mongo.stop();
  });

  beforeEach(async function () {
    this.timeout(10000);
    this.server = await Server({
      mongodbEndpoint: this.mongo.getMongouri(uuidv4()),
    });
    await this.server.ready();
  });

  afterEach(async function () {
    await this.server.stop();
  });

  it('should return tokens only when using correct email and password', async function () {
    this.server.createUserAccount(exampleUser);

    const wrongEmail = this.server.createUserToken({ email: 'thisisnotanemail@gmail.com', password: exampleUser.password });
    await expect(wrongEmail).to.be.rejected;

    const wrongPassword = this.server.createUserToken({ email: exampleUser.email, password: "wrong" });
    await expect(wrongPassword).to.be.rejected;

    const { queryToken, refreshToken } = await this.server.createUserToken({ email: exampleUser.email, password: exampleUser.password });

    jwt.verify(queryToken, global.publicKey);
    expect(refreshToken).to.be.ok;
  });

  it('should return a query token when authenticating with a valid refresh token', async function () {
    const { refreshToken } = await this.server.createUserAccount(exampleUser);

    await expect(this.server.createRefreshedToken({ refreshToken: "abc123" })).to.be.rejected;

    const queryToken = await this.server.createRefreshedToken({ refreshToken });
    jwt.verify(queryToken, global.publicKey);
  });

  it('should prevent early reauthentication with the same refresh token');
});
