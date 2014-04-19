-module(coffer_blob).

-export([parse_ref/1, validate_ref/1]).
-export([to_path/2, from_path/2]).


%% @doc parse a blob reference
-spec parse_ref(binary()) -> {HashType::binary(), Hash::binary()} | error.
parse_ref(Ref) ->
    Re = get_blob_regexp(),
    case re:run(Ref, Re, [{capture, all, binary}]) of
        {match, [_, HashType, Hash]} when byte_size(Hash) >= 3  ->
            {HashType, Hash};
        _ ->
            error
    end.

%% @doc validate a blob reference. A blob reference should be under the
%% format `<hashtype>-<hash>'
-spec validate_ref(binary()) -> ok | error.
validate_ref(Ref) ->
    Re = get_blob_regexp(),
    case re:run(Ref, Re, [{capture, all, binary}]) of
        nomatch ->
            error;
        {match, [_, _HashType, Hash]} when byte_size(Hash) >= 3 ->
            ok;
        _ ->
            error
    end.

%% @doc get local path for a blob reference when stored with the simple
%% file blob storage.
-spec to_path(Root::binary(), BlobRef::binary()) -> Path::binary() | badarg.
to_path(Root, BlobRef) ->
    case parse_ref(BlobRef) of
        {HashType, Hash} ->
            << A:1/binary, B:1/binary, C:1/binary, FName/binary >> = Hash,
            filename:join([Root, HashType, A, B, C, FName]);
        _ ->
            badarg
    end.

%% @doc return a blob reference from a local path.
-spec from_path(Root::binary(), Path::binary()) -> BlobRef::binary() | badarg.
from_path(Root, Path) ->
    [_, RelPath] = re:split(Path, Root, [{return, list}]),
    [HashType|Rest] = string:tokens(RelPath, "/"),
    iolist_to_binary([HashType, "-", [C || C <- Rest]]).



%% internal

get_blob_regexp() ->
    %% we cache the regexp so it can be reused in the same process
    case get(blob_regexp) of
        undefined ->
            {ok, RegExp} = re:compile("^([a-z][a-zA-Z0-9^-]*)-([a-zA-Z0-9]*)$"),
            put(blob_regexp, RegExp),
            RegExp;
        RegExp ->
            RegExp
    end.
