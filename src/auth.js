import { User } from 'is-able';

const refreshGracePeriod = 60 * 5 * 1000; // 5 minute grace period for refreshing a token before it expires

export const auth = token => new User({
  async: true,
  rules: {
    'create user account': () => true,
    'edit user account': id => id && (token?.aid === id || token?.pems?.auth?.acct?.usr?.edit),
    'view user account': id => id && (token?.aid === id || token?.pems?.auth?.acct?.usr?.view),
    'delete user account': id => id && (token?.aid === id || token?.pems?.auth?.acct?.usr?.delete),

    'create user token': (account, password) => account?.authenticate(password),
    'revoke user token': id => (id && token?.aid === id) || token?.pems?.auth?.tkn?.usr?.revoke,
    'update user token': account => account,

    'create user password reset token': () => token?.pems?.auth?.tkn?.createResetPass,
    'reset user password': aid => aid && token?.pems?.auth?.acct.usr?.resetPass === aid,

    'create service account': () => token?.pems?.auth?.acct?.svc?.create,
    'edit service account': () => token?.pems?.auth?.acct?.svc?.edit,
    'view service account': () => token?.pems?.auth?.acct?.svc?.view,
    'delete service account': () => token?.pems?.auth?.acct?.svc?.delete,

    'create service token': ({ domains }) => token?.pems?.auth?.tkn?.svc?.create === true || (token?.pems?.auth?.tkn?.svc?.create instanceof Array && domains?.every?.(domain => token.pems.auth.tkn.svc.create.includes(domain))),
    'revoke service token': () => token?.pems?.auth?.tkn?.svc?.revoke,

    'refresh token': refreshToken => refreshToken && (!refreshToken.refreshToken.expiration || new Date() < refreshToken.refreshToken.expiration),
    'within token refresh period': expiration => new Date() >= expiration - refreshGracePeriod,

    'set account permissions': domain => token?.pems?.auth?.acct?.pem?.edit === true || (token?.pems?.auth?.acct?.pem?.edit instanceof Array && token.pems.auth.acct.pem.edit.includes(domain)),
    'view account permissions': domain => token?.pems?.auth?.acct?.pem?.view === true || (token?.pems?.auth?.acct?.pem?.view instanceof Array && token.pems.auth.acct.pem.view.includes(domain)),

    'logged in to user account': () => token?.aid,
  },
});
