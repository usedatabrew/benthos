package httpclient

import (
	"github.com/usedatabrew/benthos/v4/public/service"
)

// AuthFieldSpecs returns a map of field specs for an auth type.
func AuthFieldSpecs() []*service.ConfigField {
	return []*service.ConfigField{
		oAuthFieldSpec(),
		BasicAuthField(),
		jwtFieldSpec(),
	}
}

// AuthFieldSpecsExpanded includes OAuth2 and JWT fields that might not be
// appropriate for all components.
func AuthFieldSpecsExpanded() []*service.ConfigField {
	return []*service.ConfigField{
		oAuthFieldSpec(),
		oAuth2FieldSpec(),
		BasicAuthField(),
		jwtFieldSpec(),
	}
}

//------------------------------------------------------------------------------

// BasicAuthField returns a config field spec for basic authentication.
func BasicAuthField() *service.ConfigField {
	return service.NewObjectField("basic_auth",
		service.NewBoolField("enabled").
			Description("Whether to use basic authentication in requests.").
			Default(false),

		service.NewStringField("username").
			Description("A username to authenticate as.").
			Default(""),

		service.NewStringField("password").
			Description("A password to authenticate with.").
			Default("").Secret(),
	).Description("Allows you to specify basic authentication.").
		Advanced()
}

func oAuthFieldSpec() *service.ConfigField {
	return service.NewObjectField("oauth",
		service.NewBoolField("enabled").
			Description("Whether to use OAuth version 1 in requests.").
			Default(false),

		service.NewStringField("consumer_key").
			Description("A value used to identify the client to the service provider.").
			Default(""),

		service.NewStringField("consumer_secret").
			Description("A secret used to establish ownership of the consumer key.").
			Default("").Secret(),

		service.NewStringField("access_token").
			Description("A value used to gain access to the protected resources on behalf of the user.").
			Default(""),

		service.NewStringField("access_token_secret").
			Description("A secret provided in order to establish ownership of a given access token.").
			Default("").Secret(),
	).
		Description("Allows you to specify open authentication via OAuth version 1.").
		Advanced()
}

func oAuth2FieldSpec() *service.ConfigField {
	return service.NewObjectField("oauth2",
		service.NewBoolField("enabled").
			Description("Whether to use OAuth version 2 in requests.").
			Default(false),

		service.NewStringField("client_key").
			Description("A value used to identify the client to the token provider.").
			Default(""),

		service.NewStringField("client_secret").
			Description("A secret used to establish ownership of the client key.").
			Default("").Secret(),

		service.NewURLField("token_url").
			Description("The URL of the token provider.").
			Default(""),

		service.NewStringListField("scopes").
			Description("A list of optional requested permissions.").
			Default([]string{}).
			Advanced().
			Version("3.45.0"),

		service.NewAnyMapField("endpoint_params").
			Description("A list of optional endpoint parameters, values should be arrays of strings.").
			Default(map[string][]string{}).
			Advanced().
			Example(map[string]any{
				"foo": []string{"meow", "quack"},
				"bar": []string{"woof"},
			}).
			Version("4.21.0").
			LintRule(`
root = if this.type() == "object" {
  this.values().map_each(ele -> if ele.type() != "array" {
    "field must be an object containing arrays of strings, got %s (%v)".format(ele.format_json(no_indent: true), ele.type())
  } else {
    ele.map_each(str -> if str.type() != "string" {
      "field values must be strings, got %s (%v)".format(str.format_json(no_indent: true), str.type())
    } else { deleted() })
  }).
    flatten()
}
`),
	).
		Description("Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.").
		Advanced()
}

func jwtFieldSpec() *service.ConfigField {
	return service.NewObjectField("jwt",
		service.NewBoolField("enabled").
			Description("Whether to use JWT authentication in requests.").
			Default(false),

		service.NewStringField("private_key_file").
			Description("A file with the PEM encoded via PKCS1 or PKCS8 as private key.").
			Default(""),

		service.NewStringField("signing_method").
			Description("A method used to sign the token such as RS256, RS384, RS512 or EdDSA.").
			Default(""),

		service.NewAnyMapField("claims").
			Description("A value used to identify the claims that issued the JWT.").
			Default(map[string]any{}).
			Advanced(),

		service.NewAnyMapField("headers").
			Description("Add optional key/value headers to the JWT.").
			Default(map[string]any{}).
			Advanced(),
	).
		Description("BETA: Allows you to specify JWT authentication.").
		Advanced()
}

//------------------------------------------------------------------------------

// AuthSignerFromParsed takes a parsed config which is expected to contain
// fields from AuthFields, and returns a RequestSigner that implements the
// configured authentication strategies by enriching a request directly.
func AuthSignerFromParsed(conf *service.ParsedConfig) (RequestSigner, error) {
	oldConf, err := authConfFromParsed(conf)
	if err != nil {
		return nil, err
	}
	return oldConf.Sign, nil
}

func authConfFromParsed(conf *service.ParsedConfig) (oldConf AuthConfig, err error) {
	oldConf = NewAuthConfig()
	if oldConf.OAuth, err = oauthFromParsed(conf); err != nil {
		return
	}
	if oldConf.BasicAuth, err = basicAuthFromParsed(conf); err != nil {
		return
	}
	if oldConf.JWT, err = jwtAuthFromParsed(conf); err != nil {
		return
	}
	return
}

func oauthFromParsed(conf *service.ParsedConfig) (res OAuthConfig, err error) {
	res = NewOAuthConfig()
	if !conf.Contains("oauth") {
		return
	}
	conf = conf.Namespace("oauth")
	if res.Enabled, err = conf.FieldBool("enabled"); err != nil {
		return
	}
	if res.ConsumerKey, err = conf.FieldString("consumer_key"); err != nil {
		return
	}
	if res.ConsumerSecret, err = conf.FieldString("consumer_secret"); err != nil {
		return
	}
	if res.AccessToken, err = conf.FieldString("access_token"); err != nil {
		return
	}
	if res.AccessTokenSecret, err = conf.FieldString("access_token_secret"); err != nil {
		return
	}
	return
}

func basicAuthFromParsed(conf *service.ParsedConfig) (res BasicAuthConfig, err error) {
	res = NewBasicAuthConfig()
	if !conf.Contains("basic_auth") {
		return
	}
	conf = conf.Namespace("basic_auth")
	if res.Enabled, err = conf.FieldBool("enabled"); err != nil {
		return
	}
	if res.Username, err = conf.FieldString("username"); err != nil {
		return
	}
	if res.Password, err = conf.FieldString("password"); err != nil {
		return
	}
	return
}

func jwtAuthFromParsed(conf *service.ParsedConfig) (res JWTConfig, err error) {
	res = NewJWTConfig()
	if !conf.Contains("jwt") {
		return
	}
	conf = conf.Namespace("jwt")
	if res.Enabled, err = conf.FieldBool("enabled"); err != nil {
		return
	}
	var claimsConfs map[string]*service.ParsedConfig
	if claimsConfs, err = conf.FieldAnyMap("claims"); err != nil {
		return
	}
	for k, v := range claimsConfs {
		if res.Claims[k], err = v.FieldAny(); err != nil {
			return
		}
	}
	var headersConfs map[string]*service.ParsedConfig
	if headersConfs, err = conf.FieldAnyMap("headers"); err != nil {
		return
	}
	for k, v := range headersConfs {
		if res.Headers[k], err = v.FieldAny(); err != nil {
			return
		}
	}
	if res.SigningMethod, err = conf.FieldString("signing_method"); err != nil {
		return
	}
	if res.PrivateKeyFile, err = conf.FieldString("private_key_file"); err != nil {
		return
	}
	return
}
