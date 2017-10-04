import { expect } from 'chai';

describe('Greeting', function () {
  describe('greeting.say', function () {
    it('should give jimmy greeting', async function () {
      const result = await request(`
        {
          greeting {
            say
          }
        }
      `);
      console.log(result);
      //expect(result).to.equal(-1, [1,2,3].indexOf(4));
    });
  });
});
