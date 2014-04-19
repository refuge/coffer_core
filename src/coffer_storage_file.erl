-module(coffer_storage_file).
-behaviour(gen_server).


-include_lib("kernel/include/file.hrl").

-export([start_link/1]).
-export([store/3, fetch/2, delete/2, enumerate/3, all/1, stat/2,
         stat_fold/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(st, {root}).


start_link(Options) ->
    gen_server:start_link(?MODULE, [Options], []).


%% @doc store a blob to the filesystem
-spec store(pid() | atom(), binary(), binary() | iolist())
    -> ok | {error, term()}.
store(Server, BlobRef, Data) ->
    gen_server:call(Server, {store, BlobRef, Data}).

%% @doc fetch a blob from the filesystem
-spec fetch(pid() | atom(), binary()) -> {ok, binary()} | {error, term()}.
fetch(Server, BlobRef) ->
    gen_server:call(Server, {fetch, BlobRef}).

%% @doc delete a blob from the filesystem
-spec delete(pid() | atom(), binary()) -> {ok, binary()} | {error, term()}.
delete(Server, BlobRef) ->
    gen_server:call(Server, {delete, BlobRef}).

%% @doc enumerate blobs on the filesystems
-spec enumerate(pid() | atom(), function(), term()) -> any().
enumerate(Server, Fun, Init) ->
    Root = gen_server:call(Server, get_root),
    do_enumerate(filelib:wildcard("*", binary_to_list(Root)),
                 binary_to_list(Root), Root, Fun, Init).

%% @doc get all blobs ont the file system
-spec all(pid() | atom()) -> [{BlobRef::binary(), Size::integer()}].
all(Server) ->
    enumerate(Server, fun(BlobInfo, Acc) ->
                {ok, [BlobInfo | Acc]}
        end, []).

%% @doc check from a list of blobs the one missings or stored on the
%% filesystem
-spec stat(pid() | atom(), [binary()] | binary()) -> {[binary()], [binary()]}.
stat(Server, BlobRef) when is_binary(BlobRef) ->
    stat(Server, [BlobRef]);
stat(Server, BlobRefs0) ->
    stat_fold(Server, BlobRefs0, fun({Type, Info}, {F, M}) ->
                case Type of
                    found ->
                        {[Info | F], M};
                    _ ->
                        {F, [Info | M]}
                end
        end, {[], []}).

%% @doc check from a list of blobs the one missings or stored on the
%% filesystem and pass the result to a function
-spec stat_fold(pid() | atom(), [binary()], function(), any()) -> any().
stat_fold(Server, BlobRefs0, Fun, Acc0) ->
    Root = gen_server:call(Server, get_root),
    %% before stating anything check the blob refs
    BlobRefs = lists:foldl(fun(BlobRef, Acc) ->
                    case coffer_blob:validate_ref(BlobRef) of
                        ok ->
                            Acc ++ [BlobRef];
                        error ->
                            Acc
                    end
            end, [], BlobRefs0),

    %% find missing
    lists:foldl(fun(BlobRef, Acc) ->
                BlobPath = coffer_blob:to_path(Root, BlobRef),
                case filelib:is_file(BlobPath) of
                    true ->
                        BlobInfo = {BlobRef, file_size(BlobPath)},
                        Fun({found, BlobInfo}, Acc);
                    false ->
                        Fun({missing, BlobRef}, Acc)
                end
        end, Acc0, BlobRefs).

%% gen_server callbacks
init([Options]) ->
    case lists:keyfind(path, 1, Options) of
        false ->
            {error, nopath};
        {path, Path} ->
            %% create the directory if it's not existing
            case filelib:ensure_dir(filename:join(Path, "test")) of
                ok ->
                    {ok, #st{root=Path}};
                Error ->
                    Error
            end
    end.

handle_call({store, BlobRef, Data}, _From, #st{root=Root}=State) ->
    Reply = do_receive(Root, BlobRef, Data),
    {reply, Reply, State};
handle_call({fetch, BlobRef}, _From, #st{root=Root}=State) ->
    Reply = do_fetch(Root, BlobRef),
    {reply, Reply, State};
handle_call({delete, BlobRef}, _From, #st{root=Root}=State) ->
    Reply = do_delete(Root, BlobRef),
    {reply, Reply, State};
handle_call(get_root, _From, #st{root=Root}=State) ->
    {reply, Root, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.



%% internal functions

do_receive(Root, BlobRef, Data) ->
    with_blobref(Root, BlobRef, fun
            ({not_found, BlobPath}) ->
                case filelib:ensure_dir(BlobPath) of
                    ok ->
                        TmpFile = iolist_to_binary([BlobPath, ".",
                                                    uniqid(), ".upload"]),
                        case file:write_file(TmpFile, Data) of
                            ok ->
                                file:rename(TmpFile, BlobPath);
                            Error ->
                                catch file:delete(TmpFile),
                                Error
                        end;
                    Error ->
                        Error
                end;
            (_) ->
                {error, already_exists}
        end).

do_fetch(Root, BlobRef) ->
    with_blobref(Root, BlobRef, fun
            ({not_found, _}) ->
                {error, not_found};
            ({_, BlobPath}) ->
                file:read_file(BlobPath)
        end).


do_delete(Root, BlobRef) ->
    with_blobref(Root, BlobRef, fun
            ({not_found, _}) ->
                {error, not_found};
            ({_, BlobPath}) ->
                DelFile = iolist_to_binary([BlobPath, ".", uniqid(), ".del"]),
                %% rename the file before deleting it
                case file:rename(BlobPath, DelFile) of
                    ok -> file:delete(DelFile);
                    Error ->
                        Error
                end
        end).

do_enumerate([], _Dir, _Root, _Fun, Acc) ->
    lists:reverse(Acc);
do_enumerate([Path | Rest], Dir, Root, Fun, Acc) ->
    Path1 = filename:join(Dir, Path),
    case filelib:is_dir(Path1) of
        true ->
            do_enumerate(filelib:wildcard("*", Path1), Path1, Root, Fun, Acc);
        _ ->
            case filelib:is_file(Path1) of
                true ->
                    Size = file_size(Path1),
                    BlobRef = coffer_blob:from_path(Root, list_to_binary(Path1)),
                    case Fun({BlobRef, Size}, Acc) of
                        {ok, Acc1} ->
                            do_enumerate(Rest, Dir, Root, Fun, Acc1);
                        stop ->
                            {ok, Acc}
                    end;
                false ->
                    do_enumerate(Rest, Dir, Root, Fun, Acc)
            end
    end.

with_blobref(Root, BlobRef, Fun) ->
    case coffer_blob:to_path(Root, BlobRef) of
        badarg -> {error, badarg};
        BlobPath ->
            case filelib:is_file(BlobPath) of
                true -> Fun({exists, BlobPath});
                false -> Fun({not_found, BlobPath})
            end
    end.

file_size(Path) ->
    {ok, FileInfo} = file:read_file_info(Path),
    #file_info{size=Size} = FileInfo,
    Size.

uniqid() ->
    integer_to_list(erlang:phash2(make_ref())).
