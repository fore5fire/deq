import chai, { expect } from 'chai';
import Mongo from 'mongo-in-memory';
import portfinder from 'portfinder';
import jwt from 'jsonwebtoken';
import chaiAsPromised from 'chai-as-promised';
import uuidv4 from 'uuid/v4';

chai.use(chaiAsPromised);

const exampleUser = {
  email: 'example@example.com',
  names: {
    first: 'Example',
    last: 'User'
  },
  password: 'This is actually a secure password',
};
const exampleUser2 = {
  email: 'example2@gmail.com',
  names: {
    first: 'Another',
    last: 'User',
  },
  password: 'This is a different secure password',
};
const exampleUser3 = {
  email: 'numba3@yahoo.com',
  names: {
    first: 'Example',
    last: 'User'
  },
  password: 'This is a different secure password',
};



describe('Account', function () {

  before(async function () {
    this.timeout(10000);
    const port = await portfinder.getPortPromise();
    log.debug({ mongoPort: port });
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
    this.timeout(10000);
    await this.server.stop();
  });

  describe('User Account', function () {

    it('should create a user account', async function () {

      const { account, queryToken, refreshToken } = await this.server.createUserAccount({ input: exampleUser });

      expect(account).to.have.property('id');
      expect(account).to.deep.equal({ email: exampleUser.email, names: exampleUser.names, id: account.id });
      jwt.verify(queryToken, global.publicKey);
    });

    it('should query user account details');

    it('should edit a user account', function () {

    });

    it('should require user emails to be unique', async function () {
      await this.server.createUserAccount({ input: exampleUser });
      await expect(this.server.createUserAccount({ input: exampleUser })).to.be.rejected;
      await this.server.createUserAccount({ input: exampleUser2 });
      await this.server.createUserAccount({ input: exampleUser3 });
      await expect(this.server.createUserAccount({ input: exampleUser2 })).to.be.rejected;
      await expect(this.server.createUserAccount({ input: exampleUser2 })).to.be.rejected;
      await expect(this.server.createUserAccount({ input: exampleUser3 })).to.be.rejected;
    });

    it('should require password strength to score at least 3 using zxcvbn');
  });

  describe("Account Access", function() {

    it('should not allow unathenticated query to access accounts');

    it("should allow access to user's own account", async function () {

      const { account, queryToken } = await this.server.createUserAccount({ input: exampleUser });

      const noId = await this.server.getAccount({ queryToken });
      const withId = await this.server.getAccount({ queryToken, id: account.id });
      console.log(noId);
      console.log(withId);
      expect(noId.id).to.equal(account.id);
      expect(withId.id).to.equal(account.id);
    });

    it("should not allow non-auth-admins access to other user's accounts");

    it("should allow auth admins to access other users accounts");
  });

  describe('Permissions', function () {
    it('should query permissions, optionally filtered by scope');
    it('should not allow non-auth-admins to add or remove permissions');
    it('should allow auth admins to add and remove permissions');
  });
});

describe('Bootstrap Admin', function () {

  before(async function () {
    this.timeout(10000);
    const port = await portfinder.getPortPromise();
    log.debug({ mongoPort: port });
    this.mongo = new Mongo(port);
    await this.mongo.start();
  });

  it('should create only one bootstrap admin', async function () {

    this.timeout(10000);

    const email = 'testAdmin@example.com';
    const password = 'This is a really secure password';
    const db = uuidv4();
    const email2 = 'test2@admin.com';

    this.server = await Server({
      mongodbEndpoint: this.mongo.getMongouri(db),
      bootstrapAdminEmail: email,
      bootstrapAdminPassword: password,
    });
    await this.server.ready();

    const token = await this.server.createUserToken({ email, password });
    expect(token).to.be.ok;

    await this.server.stop();
    this.server = await Server({
      mongodbEndpoint: this.mongo.getMongouri(db),
      bootstrapAdminEmail: email2,
      bootstrapAdminPassword: password,
    });

    expect(this.server.createUserToken({ email2, password })).to.be.rejected;

  });

  after(async function () {
    await this.server?.stop();
    await this.mongo.stop();
  });
});
