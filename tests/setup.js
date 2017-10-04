import fetch from 'node-fetch';

global.request = async (query, variables) => {
  const result = await fetch(global.config.endpoint, {
    method: 'POST',
    body: JSON.stringify({ query, variables, operationName: null }),
    headers: { 'Content-Type': 'application/json' }
  });
  
  const json = await result.json();

  if (json.errors) {
    throw json.errors;
  }
  return json.data;
};

global.config = {
  endpoint: 'http://localhost:3000/graphql',
};

before(function() {
});

after(function() {
});
