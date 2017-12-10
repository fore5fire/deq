import { User } from 'is-able';

const refreshGracePeriod = 60 * 5 * 1000; // 5 minute grace period for refreshing a token before it expires

export const auth = token => new User({
  async: true,
  rules: {
    'create user account': () => true,
    'edit user account': id => token?.aid === id || token?.pems?.auth?.uacct?.edit,
    'view user account': id => token?.aid === id || token?.pems?.auth?.uacct?.view,
    'delete user account': id => token?.aid === id || token?.pems?.auth?.uacct?.delete,

    'create user token': (account, password) => account?.authenticate(password),
    'revoke user token': id => token?.aid === id || token?.pems?.auth?.utoken?.revoke,

    'create service account': () => token?.pems?.auth?.sacct?.create,
    'edit service account': () => token?.pems?.auth?.sacct?.edit,
    'view service account': () => token?.pems?.auth?.sacct?.view,
    'delete service account': () => token?.pems?.auth?.sacct?.delete,

    'create service token': () => token?.pems?.auth?.stoken?.create,
    'revoke service token': () => token?.pems?.auth?.stoken?.revoke,

    'refresh token': refreshToken => refreshToken,
    'within token refresh period': expiration => new Date() >= expiration - refreshGracePeriod,

    'set account permissions': domain => token?.pems?.auth?.pems?.set === true || (token?.pems?.auth?.pems?.set/* */instanceof Array && token.pems.auth.pems.set.includes(domain)),
    'view account permissions': domain => token?.pems?.auth?.pems?.view === true || (token?.pems?.auth?.pems?.view instanceof Array && token.pems.auth.pems.view.includes(domain)),

    'logged in to user account': () => token?.aid,
  },
});
