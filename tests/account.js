import { expect, use } from 'chai';
import Mongo from 'mongo-in-memory';
import portfinder from 'portfinder';
import jwt from 'jsonwebtoken';
import chaiAsPromised from 'chai-as-promised';
import chaiExclude from 'chai-exclude';
import uuidv4 from 'uuid/v4';

use(chaiAsPromised);
use(chaiExclude);

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

const bootstrapAdmin = {
  email: 'testadmin@example.com',
  password: 'This is a really secure password',
  names: {
    first: 'Bootstrap',
    last: 'Admin',
  }
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
      bootstrapAdminEmail: bootstrapAdmin.email,
      bootstrapAdminPassword: bootstrapAdmin.password,
    });
    await this.server.ready();
  });

  afterEach(async function () {
    this.timeout(10000);
    await this.server.stop();
  });


  it.only('should list accounts', async function () {

    await this.server.createUserAccount({ input: exampleUser });
    await this.server.createUserAccount({ input: exampleUser2 });
    await this.server.createUserAccount({ input: exampleUser3 });

    const { queryToken } = await this.server.createUserToken({ email: bootstrapAdmin.email, password: bootstrapAdmin.password });

    const accounts = await this.server.getAccounts({ queryToken });

    expect(accounts).to.have.length(4); // 4 accounts including the admin account

    expect(accounts).excluding(['password', 'id', 'email', 'names']).to.deep.equal([ bootstrapAdmin, exampleUser, exampleUser2, exampleUser3 ]);

    const filter = {
      cursor: {
        limit: 2,
      }
    };

    const { data, next, previous } = await this.server.getAccounts({ queryToken, filter });
  });

  describe('User Account', function () {

    it('should create a user account', async function () {

      const { account, queryToken, refreshToken } = await this.server.createUserAccount({ input: exampleUser });

      expect(account).to.have.property('id');
      expect(account).to.deep.equal({ email: exampleUser.email, names: exampleUser.names, id: account.id });
      jwt.verify(queryToken, global.publicKey);
    });

    it('should query user account details');

    it('should list user accounts', async function () {

      await this.server.createUserAccount({ input: exampleUser });
      await this.server.createUserAccount({ input: exampleUser2 });
      await this.server.createUserAccount({ input: exampleUser3 });

      const { queryToken } = await this.server.createUserToken({ email: bootstrapAdmin.email, password: bootstrapAdmin.password });

      const accounts = await this.server.getUserAccounts({ queryToken });

      expect(accounts).to.have.length(4); // 4 accounts including the admin account
      expect(accounts).excluding(['password', 'id']).to.deep.equal([ bootstrapAdmin, exampleUser, exampleUser2, exampleUser3 ]);

    });

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

    it('should change user password', async function () {

      const input = {
        permissions: [{
          domain: 'auth',
          value: { "tkn": { "createResetPass": true } }
        }]
      };

      const newPassword = 'A new very secure password';

      const adminToken = await this.server.createUserToken({ email: bootstrapAdmin.email, password: bootstrapAdmin.password });
      const serviceToken = await this.server.createServiceToken({ input, queryToken: adminToken.queryToken });

      await this.server.createUserAccount({ input: exampleUser });
      const { resetToken } = await this.server.createPasswordResetToken({ email: exampleUser.email, queryToken: serviceToken.queryToken });
      const { account, queryToken } = await this.server.changePassword({ resetToken, email: exampleUser.email, newPassword });

      expect(queryToken).to.be.ok;
      expect(account).to.be.ok;
      const { id } = await this.server.getAccount({ queryToken });
      expect(id).to.equal(account.id);

      expect(this.server.createUserToken({ email: exampleUser.email, password: exampleUser.password })).to.be.rejected;
      const newToken = await this.server.createUserToken({ email: exampleUser.email, password: newPassword });
      expect(newToken.queryToken).to.be.ok;
    });
  });

  describe("Account Access", function () {

    it('should not allow unathenticated query to access accounts');

    it("should allow access to user's own account", async function () {

      const { account, queryToken } = await this.server.createUserAccount({ input: exampleUser });

      const noId = await this.server.getAccount({ queryToken });
      const withId = await this.server.getAccount({ queryToken, id: account.id });

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

    const db = uuidv4();
    const email = 'test2@admin.com';
    const password = 'ftygyunuygntgytfjytfjyt bybuyg';

    this.server = await Server({
      mongodbEndpoint: this.mongo.getMongouri(db),
      bootstrapAdminEmail: bootstrapAdmin.email,
      bootstrapAdminPassword: bootstrapAdmin.password,
    });
    await this.server.ready();

    const token = await this.server.createUserToken({ email: bootstrapAdmin.email, password: bootstrapAdmin.password });
    expect(token).to.be.ok;

    await this.server.stop();
    this.server = await Server({
      mongodbEndpoint: this.mongo.getMongouri(db),
      bootstrapAdminEmail: email,
      bootstrapAdminPassword: password,
    });

    expect(this.server.createUserToken({ email, password })).to.be.rejected;

  });

  it('should not be case sensitive');

  after(async function () {
    await this.server?.stop();
    await this.mongo.stop();
  });
});
