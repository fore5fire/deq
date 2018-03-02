import chai, { expect } from 'chai';
import jwt from 'jsonwebtoken';
import chaiAsPromised from 'chai-as-promised';

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


describe('User Token', function () {


  it('should return tokens only when using correct email and password', async function () {
    Server.createUserAccount({ input: exampleUser });

    const wrongEmail = Server.createUserToken({ email: 'thisisnotanemail@gmail.com', password: exampleUser.password });
    await expect(wrongEmail).to.be.rejected;

    const wrongPassword = Server.createUserToken({ email: exampleUser.email, password: "wrong" });
    await expect(wrongPassword).to.be.rejected;

    const { queryToken, refreshToken } = await Server.createUserToken({ email: exampleUser.email, password: exampleUser.password });

    jwt.verify(queryToken, global.publicKey);
    expect(refreshToken).to.be.ok;
  });

  it('should return a query token when authenticating with a valid refresh token', async function () {
    const { refreshToken } = await Server.createUserAccount({ input: exampleUser });

    await expect(Server.createRefreshedToken({ refreshToken: "abc123" })).to.be.rejected;

    const queryToken = await Server.createRefreshedToken({ refreshToken });
    jwt.verify(queryToken, global.publicKey);
  });

  it('should prevent early reauthentication with the same refresh token');
});

describe('Service Token', function () {
  //
  // beforeEach(async function () {
  //   this.timeout(10000);
  //
  //   this.email = 'admin@example.com';
  //   this.password = 'This is a highly secure password';
  //
  //   Server = await Server({
  //     bootstrapAdminEmail: 'admin@example.com',
  //     bootstrapAdminPassword: 'This is a highly secure password',
  //   });
  //   await Server.ready();
  // });
  //
  // afterEach(async function () {
  //   await Server.stop();
  // });
  //
  //
  // it('should be created', async function () {
  //
  //   const pancakePems = {
  //     make: true,
  //     eat: false,
  //     apply: ['abc', 123]
  //   };
  //
  //   const carPems = {
  //     abc: 123
  //   };
  //
  //   const { queryToken } = await Server.createUserToken({ email: this.email, password: this.password });
  //   const input = {
  //     permissions: [
  //       { domain: 'pancakes', value: pancakePems },
  //       { domain: 'cars', value: carPems },
  //     ]
  //   };
  //
  //   const serviceToken = await Server.createServiceToken({ queryToken, input });
  //
  //   const { pems } = jwt.verify(serviceToken.queryToken, publicKey);
  //   expect(pems).to.deep.equal({ pancakes: pancakePems, cars: carPems });
  // });
});
