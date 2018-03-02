import { expect, use } from 'chai';
import jwt from 'jsonwebtoken';
import chaiAsPromised from 'chai-as-promised';
import chaiExclude from 'chai-exclude';

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

  it('should list accounts', async function () {

    await Server.createUserAccount({ input: exampleUser });
    await Server.createUserAccount({ input: exampleUser2 });
    await Server.createUserAccount({ input: exampleUser3 });

    const { queryToken } = await Server.createUserToken({ email: bootstrapAdmin.email, password: bootstrapAdmin.password });

    const accounts = await Server.getAccounts({ queryToken });

    expect(accounts).to.have.length(4); // 4 accounts including the admin account

    const expected = [bootstrapAdmin, exampleUser, exampleUser2, exampleUser3].map(user => ({ name: `${user.names.first} ${user.names.last}` }));

    expect(accounts).excluding(['password', 'id', 'email', 'names']).to.deep.equal(expected);

    const filter = {
      cursor: {
        limit: 2,
      }
    };

    const { data, next, previous } = await Server.getAccounts({ queryToken, filter });
  });

  describe('User Account', function () {

    it('should create a user account', async function () {

      const { account, queryToken, refreshToken } = await Server.createUserAccount({ input: exampleUser });

      expect(account).to.have.property('id');
      expect(account).to.deep.equal({ email: exampleUser.email, names: exampleUser.names, id: account.id });
      jwt.verify(queryToken, global.publicKey);
    });

    it('should query user account details');

    it.only('should list user accounts', async function () {

      await Server.createUserAccount({ input: exampleUser });
      await Server.createUserAccount({ input: exampleUser2 });
      await Server.createUserAccount({ input: exampleUser3 });

      const { queryToken } = await Server.createUserToken({ email: bootstrapAdmin.email, password: bootstrapAdmin.password });

      const accounts = await Server.getUserAccounts({ queryToken });

      expect(accounts).to.have.length(4); // 4 accounts including the admin account
      expect(accounts).excluding(['password', 'id']).to.deep.equal([ bootstrapAdmin, exampleUser, exampleUser2, exampleUser3 ]);

    });

    it('should edit a user account', function () {

    });

    it('should require user emails to be unique', async function () {
      await Server.createUserAccount({ input: exampleUser });
      await expect(Server.createUserAccount({ input: exampleUser })).to.be.rejected;
      await Server.createUserAccount({ input: exampleUser2 });
      await Server.createUserAccount({ input: exampleUser3 });
      await expect(Server.createUserAccount({ input: exampleUser2 })).to.be.rejected;
      await expect(Server.createUserAccount({ input: exampleUser2 })).to.be.rejected;
      await expect(Server.createUserAccount({ input: exampleUser3 })).to.be.rejected;
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

      const adminToken = await Server.createUserToken({ email: bootstrapAdmin.email, password: bootstrapAdmin.password });
      const serviceToken = await Server.createServiceToken({ input, queryToken: adminToken.queryToken });

      await Server.createUserAccount({ input: exampleUser });
      const { resetToken } = await Server.createPasswordResetToken({ email: exampleUser.email, queryToken: serviceToken.queryToken });
      const { account, queryToken } = await Server.changePassword({ resetToken, email: exampleUser.email, newPassword });

      expect(queryToken).to.be.ok;
      expect(account).to.be.ok;
      const { id } = await Server.getAccount({ queryToken });
      expect(id).to.equal(account.id);

      expect(Server.createUserToken({ email: exampleUser.email, password: exampleUser.password })).to.be.rejected;
      const newToken = await Server.createUserToken({ email: exampleUser.email, password: newPassword });
      expect(newToken.queryToken).to.be.ok;
    });
  });

  describe("Account Access", function () {

    it('should not allow unathenticated query to access accounts');

    it("should allow access to user's own account", async function () {

      const { account, queryToken } = await Server.createUserAccount({ input: exampleUser });

      const noId = await Server.getAccount({ queryToken });
      const withId = await Server.getAccount({ queryToken, id: account.id });

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
//
// describe('Bootstrap Admin', function () {
//
//
//   it('should create only one bootstrap admin', async function () {
//
//     this.timeout(10000);
//
//     const email = 'test2@admin.com';
//     const password = 'ftygyunuygntgytfjytfjyt bybuyg';
//
//     Server = await Server({
//       bootstrapAdminEmail: bootstrapAdmin.email,
//       bootstrapAdminPassword: bootstrapAdmin.password,
//     });
//     await Server.ready();
//
//     const token = await Server.createUserToken({ email: bootstrapAdmin.email, password: bootstrapAdmin.password });
//     expect(token).to.be.ok;
//
//     await Server.stop();
//     Server = await Server({
//       bootstrapAdminEmail: email,
//       bootstrapAdminPassword: password,
//     });
//
//     expect(Server.createUserToken({ email, password })).to.be.rejected;
//
//   });
//
//   it('should not be case sensitive');
//
//   after(async function () {
//     await Server?.stop();
//   });
// });
