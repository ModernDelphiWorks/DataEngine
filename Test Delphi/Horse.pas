unit Horse;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections;

type
  THorseRequest = class;
  THorseResponse = class;
  TNextProc = reference to procedure;
  THorseCallback = reference to procedure(Req: THorseRequest; Res: THorseResponse; Next: TNextProc);

  THorseRequest = class
  private
    FSession: TDictionary<string, TObject>;
    FHeaders: TDictionary<string, string>;
  public
    constructor Create;
    destructor Destroy; override;
    property Session: TDictionary<string, TObject> read FSession;
    property Headers: TDictionary<string, string> read FHeaders;
  end;

  THorseResponse = class
  private
    FStatus: Integer;
    FContent: string;
  public
    constructor Create;
    function Status(const AStatus: Integer): THorseResponse; overload;
    function Status: Integer; overload;
    function Send(const AContent: string): THorseResponse;
    property StatusValue: Integer read FStatus;
  end;

  THorse = class
  public
    class procedure Use(ACallback: THorseCallback);
    class procedure Get(const APath: string; ACallback: THorseCallback);
    class procedure Post(const APath: string; ACallback: THorseCallback);
    class procedure Listen(const APort: Integer = 9000; const ACallback: TProc = nil);
  end;

implementation

{ THorseRequest }

constructor THorseRequest.Create;
begin
  inherited Create;
  FSession := TDictionary<string, TObject>.Create;
  FHeaders := TDictionary<string, string>.Create;
end;

destructor THorseRequest.Destroy;
begin
  FSession.Free;
  FHeaders.Free;
  inherited;
end;

{ THorseResponse }

constructor THorseResponse.Create;
begin
  inherited Create;
  FStatus := 200;
end;

function THorseResponse.Status(const AStatus: Integer): THorseResponse;
begin
  FStatus := AStatus;
  Result := self;
end;

function THorseResponse.Status: Integer;
begin
  Result := FStatus;
end;

function THorseResponse.Send(const AContent: string): THorseResponse;
begin
  FContent := AContent;
  Result := self;
end;

{ THorse }

class procedure THorse.Use(ACallback: THorseCallback); begin end;
class procedure THorse.Get(const APath: string; ACallback: THorseCallback); begin end;
class procedure THorse.Post(const APath: string; ACallback: THorseCallback); begin end;
class procedure THorse.Listen(const APort: Integer; const ACallback: TProc); begin end;

end.
