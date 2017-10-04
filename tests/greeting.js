import { expect } from 'chai';

describe('greeting', function () {
  it('should be a Greeting', async function () {
    const result = await request(`{
      greeting {
        __typename
      }
    }`);
    
    expect(result).to.deep.equal({ greeting: { __typename: 'Greeting'} });
  });
  describe('say', function () {
    it('should give default name', async function () {
      const result = await request(`{
        greeting {
          say
        }
      }`);
      
      expect(result).to.deep.equal({ greeting: { say: 'my name is Jimmy. What\'s your name?' } });
    });
    it('should give custom name', async function () {

      const name = "Test123";

      const result = await request(`
        query IntroductionForName($name: String) {
          greeting(name: $name) {
            say
          }
        }
      `, { name });

      expect(result).to.deep.equal({ greeting: { say: `my name is ${name}. What\'s your name?` } });
    });
  });

  describe('exclaim', function () {
    it('should give default response', async function () {

      const name = 'joseph';

      const result = await request(`
        query Exclaimation($name: String!) {
        greeting {
          exclaim(name: $name)
        }
      }`, { name });
      
      expect(result).to.deep.equal({ greeting: { exclaim: `Hello ${name}!!!` } });
    });
  });
});
