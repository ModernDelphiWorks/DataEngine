{
  ------------------------------------------------------------------------------
  DataEngine
  Modular and extensible database engine framework for Delphi.

  SPDX-License-Identifier: Apache-2.0
  Copyright (c) 2025-2026 Isaque Pinheiro

  Licensed under the Apache License, Version 2.0.
  See the LICENSE file in the project root for full license information.
  ------------------------------------------------------------------------------
}

unit DataEngine.DataSetSnapshot;

interface

uses
  DB,
  Classes,
  SysUtils,
  Variants,
  DataEngine.FactoryInterfaces;

type
  TDataSetSnapshot = class(TInterfacedObject, IDBDataSetSnapshot, IDBDataSet)
  private
    FData: TDataSet; // Generic Internal DataSet (could be any TDataSet wrapper)
    FSource: IDBDataSet; // Reference to source if needed
  protected
    // IDBDataSet implementation delegating to FData
    function _GetFilter: String;
    function _GetFiltered: Boolean;
    function _GetFilterOptions: TFilterOptions;
    function _GetActive: Boolean;
    function _GetCommandText: String;
    function _GetAfterCancel: TDataSetNotifyEvent;
    function _GetAfterClose: TDataSetNotifyEvent;
    function _GetAfterDelete: TDataSetNotifyEvent;
    function _GetAfterEdit: TDataSetNotifyEvent;
    function _GetAfterInsert: TDataSetNotifyEvent;
    function _GetAfterOpen: TDataSetNotifyEvent;
    function _GetAfterPost: TDataSetNotifyEvent;
    function _GetAfterRefresh: TDataSetNotifyEvent;
    function _GetAfterScroll: TDataSetNotifyEvent;
    function _GetAutoCalcFields: Boolean;
    function _GetBeforeCancel: TDataSetNotifyEvent;
    function _GetBeforeClose: TDataSetNotifyEvent;
    function _GetBeforeDelete: TDataSetNotifyEvent;
    function _GetBeforeEdit: TDataSetNotifyEvent;
    function _GetBeforeInsert: TDataSetNotifyEvent;
    function _GetBeforeOpen: TDataSetNotifyEvent;
    function _GetBeforePost: TDataSetNotifyEvent;
    function _GetBeforeRefresh: TDataSetNotifyEvent;
    function _GetBeforeScroll: TDataSetNotifyEvent;
    function _GetOnCalcFields: TDataSetNotifyEvent;
    function _GetOnDeleteError: TDataSetErrorEvent;
    function _GetOnEditError: TDataSetErrorEvent;
    function _GetOnFilterRecord: TFilterRecordEvent;
    function _GetOnNewRecord: TDataSetNotifyEvent;
    function _GetOnPostError: TDataSetErrorEvent;
    function _GetSortFields: String;
    function _GetBookmark: TBookmark;
    function _GetBlockReadSize: Integer;
    function _GetFieldValue(const FieldName: string): Variant;
    function _GetFetchingAll: Boolean;
    function _GetFetchOptions: TFetchOptions;

    procedure _SetFilter(const Value: String);
    procedure _SetFiltered(const Value: Boolean);
    procedure _SetFilterOptions(Value: TFilterOptions);
    procedure _SetActive(const Value: Boolean);
    procedure _SetCommandText(const ACommandText: String);
    procedure _SetUniDirectional(const Value: Boolean);
    procedure _SetReadOnly(const Value: Boolean);
    procedure _SetCachedUpdates(const Value: Boolean);
    procedure _SetAfterCancel(const Value: TDataSetNotifyEvent);
    procedure _SetAfterOpen(const Value: TDataSetNotifyEvent);
    procedure _SetAfterClose(const Value: TDataSetNotifyEvent);
    procedure _SetAfterDelete(const Value: TDataSetNotifyEvent);
    procedure _SetAfterEdit(const Value: TDataSetNotifyEvent);
    procedure _SetAfterInsert(const Value: TDataSetNotifyEvent);
    procedure _SetAfterPost(const Value: TDataSetNotifyEvent);
    procedure _SetAfterRefresh(const Value: TDataSetNotifyEvent);
    procedure _SetAfterScroll(const Value: TDataSetNotifyEvent);
    procedure _SetAutoCalcFields(const Value: Boolean);
    procedure _SetBeforeCancel(const Value: TDataSetNotifyEvent);
    procedure _SetBeforeDelete(const Value: TDataSetNotifyEvent);
    procedure _SetBeforeEdit(const Value: TDataSetNotifyEvent);
    procedure _SetBeforeInsert(const Value: TDataSetNotifyEvent);
    procedure _SetBeforeOpen(const Value: TDataSetNotifyEvent);
    procedure _SetBeforeClose(const Value: TDataSetNotifyEvent);
    procedure _SetBeforePost(const Value: TDataSetNotifyEvent);
    procedure _SetBeforeRefresh(const Value: TDataSetNotifyEvent);
    procedure _SetBeforeScroll(const Value: TDataSetNotifyEvent);
    procedure _SetOnFilterRecord(const Value: TFilterRecordEvent);
    procedure _SetOnCalcFields(const Value: TDataSetNotifyEvent);
    procedure _SetOnDeleteError(const Value: TDataSetErrorEvent);
    procedure _SetOnEditError(const Value: TDataSetErrorEvent);
    procedure _SetOnNewRecord(const Value: TDataSetNotifyEvent);
    procedure _SetOnPostError(const Value: TDataSetErrorEvent);
    procedure _SetSortFields(const Value: String);
    procedure _SetBookmark(const Value: TBookmark);
    procedure _SetBlockReadSize(const Value: Integer);
    procedure _SetFieldValue(const FieldName: string; const Value: Variant);
    procedure _SetFetchingAll(const Value: Boolean);
    procedure _SetFetchOptions(const Value: TFetchOptions);

  public
    procedure Close;
    procedure Open;
    procedure OpenAsync(const AOnComplete: TProc; const AOnError: TProc<Exception> = nil);

    procedure Refresh;
    procedure Delete;
    procedure Cancel;
    procedure Clear;
    procedure DisableControls;
    procedure EnableControls;
    procedure Next;
    procedure Prior;
    procedure Append;
    procedure Insert;
    procedure Edit;
    procedure Post;
    procedure First;
    procedure Last;
    procedure MoveBy(Distance: Integer);
    procedure CheckRequiredFields;
    procedure FreeBookmark(Bookmark: TBookmark);
    procedure ClearFields;
    procedure ApplyUpdates;
    procedure CancelUpdates;
    function Locate(const KeyFields: String; const KeyValues: Variant; Options: TLocateOptions): Boolean;
    function Lookup(const KeyFields: String; const KeyValues: Variant; const ResultFields: String): Variant;
    function FieldCount: UInt16;
    function State: TDataSetState;
    function RecordCount: UInt32;
    function FieldDefs: TFieldDefs;
    function Eof: Boolean;
    function Bof: Boolean;
    function RecNo: Integer;
    function CanRefresh: Boolean;
    function DataSetField: TDataSetField;
    function DefaultFields: Boolean;
    function Designer: TDataSetDesigner;
    function FieldList: TFieldList;
    function Found: Boolean;
    function ObjectView: Boolean;
    function RecordSize: Word;
    function SparseArrays: Boolean;
    function FieldDefList: TFieldDefList;
    function Fields: TFields;
    function RowsAffected: UInt32;
    function Modified: Boolean;
    function FieldByName(const AFieldName: String): TField;
    function FindField(const AFieldName: string): TField;
    function FindFirst: Boolean;
    function FindLast: Boolean;
    function FindNext: Boolean;
    function FindPrior: Boolean;
    function AggFields: TFields;
    function UpdatesPending: Boolean;
    function UpdateStatus: TUpdateStatus;
    function CanModify: Boolean;
    function IsEmpty: Boolean;
    function IsUniDirectional: Boolean;
    function IsReadOnly: Boolean;
    function IsCachedUpdates: Boolean;
    function DataSource: TDataSource;
    function NormalizeFieldValue(const AFieldName: String; const AValue: Variant): Variant;
    function AsDataSet: TDataSet;
    function CreateView: IDBDataSet;
    constructor Create(const AInternal: TDataSet; const AIsOwner: Boolean = True);
    destructor Destroy; override;
  private
    FOwnsData: Boolean;
  end;

implementation

uses
  DataEngine.InMemoryDataFactory;

{ TDataSetSnapshot }

constructor TDataSetSnapshot.Create(const AInternal: TDataSet; const AIsOwner: Boolean);
begin
  inherited Create;
  FData := AInternal;
  FOwnsData := AIsOwner;
end;

destructor TDataSetSnapshot.Destroy;
begin
  if FOwnsData then
    FData.Free;
  inherited;
end;

procedure TDataSetSnapshot.Append; begin FData.Append; end;
procedure TDataSetSnapshot.ApplyUpdates; begin end;
function TDataSetSnapshot.AsDataSet: TDataSet; begin Result := FData; end;
function TDataSetSnapshot.Bof: Boolean; begin Result := FData.Bof; end;
procedure TDataSetSnapshot.Cancel; begin FData.Cancel; end;
procedure TDataSetSnapshot.CancelUpdates; begin end;
function TDataSetSnapshot.CanModify: Boolean; begin Result := FData.CanModify; end;
function TDataSetSnapshot.CanRefresh: Boolean; begin Result := FData.CanRefresh; end;
procedure TDataSetSnapshot.CheckRequiredFields; begin end;
procedure TDataSetSnapshot.Clear; begin end;
procedure TDataSetSnapshot.ClearFields; begin FData.ClearFields; end;
procedure TDataSetSnapshot.Close; begin FData.Close; end;
function TDataSetSnapshot.DataSource: TDataSource; begin Result := nil; end;
function TDataSetSnapshot.DataSetField: TDataSetField; begin Result := FData.DataSetField; end;
function TDataSetSnapshot.DefaultFields: Boolean; begin Result := FData.DefaultFields; end;
procedure TDataSetSnapshot.Delete; begin FData.Delete; end;
function TDataSetSnapshot.Designer: TDataSetDesigner; begin Result := FData.Designer; end;
procedure TDataSetSnapshot.DisableControls; begin FData.DisableControls; end;
procedure TDataSetSnapshot.Edit; begin FData.Edit; end;
procedure TDataSetSnapshot.EnableControls; begin FData.EnableControls; end;
function TDataSetSnapshot.Eof: Boolean; begin Result := FData.Eof; end;
function TDataSetSnapshot.FieldByName(const AFieldName: String): TField; begin Result := FData.FieldByName(AFieldName); end;
function TDataSetSnapshot.FieldCount: UInt16; begin Result := FData.FieldCount; end;
function TDataSetSnapshot.Fields: TFields; begin Result := FData.Fields; end;
function TDataSetSnapshot.FieldDefList: TFieldDefList; begin Result := FData.FieldDefList; end;
function TDataSetSnapshot.FieldDefs: TFieldDefs; begin Result := FData.FieldDefs; end;
function TDataSetSnapshot.FieldList: TFieldList; begin Result := FData.FieldList; end;
function TDataSetSnapshot.FindField(const AFieldName: string): TField; begin Result := FData.FindField(AFieldName); end;
function TDataSetSnapshot.FindFirst: Boolean; begin Result := FData.FindFirst; end;
function TDataSetSnapshot.FindLast: Boolean; begin Result := FData.FindLast; end;
function TDataSetSnapshot.FindNext: Boolean; begin Result := FData.FindNext; end;
function TDataSetSnapshot.FindPrior: Boolean; begin Result := FData.FindPrior; end;
procedure TDataSetSnapshot.First; begin FData.First; end;
function TDataSetSnapshot.Found: Boolean; begin Result := FData.Found; end;
procedure TDataSetSnapshot.FreeBookmark(Bookmark: TBookmark); begin FData.FreeBookmark(Bookmark); end;
procedure TDataSetSnapshot.Insert; begin FData.Insert; end;
function TDataSetSnapshot.IsCachedUpdates: Boolean; begin Result := False; end;
function TDataSetSnapshot.IsEmpty: Boolean; begin Result := FData.IsEmpty; end;
function TDataSetSnapshot.IsReadOnly: Boolean; begin Result := True; end;
function TDataSetSnapshot.IsUniDirectional: Boolean; begin Result := False; end;
procedure TDataSetSnapshot.Last; begin FData.Last; end;
function TDataSetSnapshot.Locate(const KeyFields: String; const KeyValues: Variant; Options: TLocateOptions): Boolean; begin Result := FData.Locate(KeyFields, KeyValues, Options); end;
function TDataSetSnapshot.Lookup(const KeyFields: String; const KeyValues: Variant; const ResultFields: String): Variant; begin Result := FData.Lookup(KeyFields, KeyValues, ResultFields); end;
function TDataSetSnapshot.Modified: Boolean; begin Result := FData.Modified; end;
procedure TDataSetSnapshot.MoveBy(Distance: Integer); begin FData.MoveBy(Distance); end;
procedure TDataSetSnapshot.Next; begin FData.Next; end;
function TDataSetSnapshot.NormalizeFieldValue(const AFieldName: String; const AValue: Variant): Variant; begin Result := AValue; end;
function TDataSetSnapshot.ObjectView: Boolean; begin Result := FData.ObjectView; end;
procedure TDataSetSnapshot.Open; begin FData.Open; end;
procedure TDataSetSnapshot.OpenAsync(const AOnComplete: TProc; const AOnError: TProc<Exception>);
begin
  try
    Open;
    if Assigned(AOnComplete) then
      AOnComplete();
  except
    on E: Exception do
      if Assigned(AOnError) then
        AOnError(E)
      else
        raise;
  end;
end;

procedure TDataSetSnapshot.Post; begin FData.Post; end;
procedure TDataSetSnapshot.Prior; begin FData.Prior; end;
function TDataSetSnapshot.RecordCount: UInt32; begin Result := FData.RecordCount; end;
function TDataSetSnapshot.RecordSize: Word; begin Result := FData.RecordSize; end;
function TDataSetSnapshot.RecNo: Integer; begin Result := FData.RecNo; end;
procedure TDataSetSnapshot.Refresh; begin FData.Refresh; end;
function TDataSetSnapshot.RowsAffected: UInt32; begin Result := 0; end;
function TDataSetSnapshot.SparseArrays: Boolean; begin Result := FData.SparseArrays; end;
function TDataSetSnapshot.State: TDataSetState; begin Result := FData.State; end;
function TDataSetSnapshot.UpdateStatus: TUpdateStatus; begin Result := usUnmodified; end;
function TDataSetSnapshot.UpdatesPending: Boolean; begin Result := False; end;
function TDataSetSnapshot.AggFields: TFields; begin Result := FData.AggFields; end;

function TDataSetSnapshot.CreateView: IDBDataSet;
var
  LNewData: TDataSet;
begin
  LNewData := TInMemoryDataFactory.CloneDataSet(FData);
  Result := TDataSetSnapshot.Create(LNewData, True);
end;

function TDataSetSnapshot._GetActive: Boolean; begin Result := FData.Active; end;
function TDataSetSnapshot._GetAfterCancel: TDataSetNotifyEvent; begin Result := FData.AfterCancel; end;
function TDataSetSnapshot._GetAfterClose: TDataSetNotifyEvent; begin Result := FData.AfterClose; end;
function TDataSetSnapshot._GetAfterDelete: TDataSetNotifyEvent; begin Result := FData.AfterDelete; end;
function TDataSetSnapshot._GetAfterEdit: TDataSetNotifyEvent; begin Result := FData.AfterEdit; end;
function TDataSetSnapshot._GetAfterInsert: TDataSetNotifyEvent; begin Result := FData.AfterInsert; end;
function TDataSetSnapshot._GetAfterOpen: TDataSetNotifyEvent; begin Result := FData.AfterOpen; end;
function TDataSetSnapshot._GetAfterPost: TDataSetNotifyEvent; begin Result := FData.AfterPost; end;
function TDataSetSnapshot._GetAfterRefresh: TDataSetNotifyEvent; begin Result := FData.AfterRefresh; end;
function TDataSetSnapshot._GetAfterScroll: TDataSetNotifyEvent; begin Result := FData.AfterScroll; end;
function TDataSetSnapshot._GetAutoCalcFields: Boolean; begin Result := FData.AutoCalcFields; end;
function TDataSetSnapshot._GetBeforeCancel: TDataSetNotifyEvent; begin Result := FData.BeforeCancel; end;
function TDataSetSnapshot._GetBeforeClose: TDataSetNotifyEvent; begin Result := FData.BeforeClose; end;
function TDataSetSnapshot._GetBeforeDelete: TDataSetNotifyEvent; begin Result := FData.BeforeDelete; end;
function TDataSetSnapshot._GetBeforeEdit: TDataSetNotifyEvent; begin Result := FData.BeforeEdit; end;
function TDataSetSnapshot._GetBeforeInsert: TDataSetNotifyEvent; begin Result := FData.BeforeInsert; end;
function TDataSetSnapshot._GetBeforeOpen: TDataSetNotifyEvent; begin Result := FData.BeforeOpen; end;
function TDataSetSnapshot._GetBeforePost: TDataSetNotifyEvent; begin Result := FData.BeforePost; end;
function TDataSetSnapshot._GetBeforeRefresh: TDataSetNotifyEvent; begin Result := FData.BeforeRefresh; end;
function TDataSetSnapshot._GetBeforeScroll: TDataSetNotifyEvent; begin Result := FData.BeforeScroll; end;
function TDataSetSnapshot._GetBlockReadSize: Integer; begin Result := FData.BlockReadSize; end;
function TDataSetSnapshot._GetBookmark: TBookmark; begin Result := FData.Bookmark; end;
function TDataSetSnapshot._GetCommandText: String; begin Result := ''; end;
function TDataSetSnapshot._GetFetchingAll: Boolean; begin Result := True; end;
function TDataSetSnapshot._GetFieldValue(const FieldName: string): Variant; begin Result := FData.FieldValues[FieldName]; end;
function TDataSetSnapshot._GetFilter: String; begin Result := FData.Filter; end;
function TDataSetSnapshot._GetFiltered: Boolean; begin Result := FData.Filtered; end;
function TDataSetSnapshot._GetFilterOptions: TFilterOptions; begin Result := FData.FilterOptions; end;
function TDataSetSnapshot._GetOnCalcFields: TDataSetNotifyEvent; begin Result := FData.OnCalcFields; end;
function TDataSetSnapshot._GetOnDeleteError: TDataSetErrorEvent; begin Result := FData.OnDeleteError; end;
function TDataSetSnapshot._GetOnEditError: TDataSetErrorEvent; begin Result := FData.OnEditError; end;
function TDataSetSnapshot._GetOnFilterRecord: TFilterRecordEvent; begin Result := FData.OnFilterRecord; end;
function TDataSetSnapshot._GetOnNewRecord: TDataSetNotifyEvent; begin Result := FData.OnNewRecord; end;
function TDataSetSnapshot._GetOnPostError: TDataSetErrorEvent; begin Result := FData.OnPostError; end;
function TDataSetSnapshot._GetSortFields: String; begin Result := ''; end;

procedure TDataSetSnapshot._SetActive(const Value: Boolean); begin FData.Active := Value; end;
procedure TDataSetSnapshot._SetAfterCancel(const Value: TDataSetNotifyEvent); begin FData.AfterCancel := Value; end;
procedure TDataSetSnapshot._SetAfterClose(const Value: TDataSetNotifyEvent); begin FData.AfterClose := Value; end;
procedure TDataSetSnapshot._SetAfterDelete(const Value: TDataSetNotifyEvent); begin FData.AfterDelete := Value; end;
procedure TDataSetSnapshot._SetAfterEdit(const Value: TDataSetNotifyEvent); begin FData.AfterEdit := Value; end;
procedure TDataSetSnapshot._SetAfterInsert(const Value: TDataSetNotifyEvent); begin FData.AfterInsert := Value; end;
procedure TDataSetSnapshot._SetAfterOpen(const Value: TDataSetNotifyEvent); begin FData.AfterOpen := Value; end;
procedure TDataSetSnapshot._SetAfterPost(const Value: TDataSetNotifyEvent); begin FData.AfterPost := Value; end;
procedure TDataSetSnapshot._SetAfterRefresh(const Value: TDataSetNotifyEvent); begin FData.AfterRefresh := Value; end;
procedure TDataSetSnapshot._SetAfterScroll(const Value: TDataSetNotifyEvent); begin FData.AfterScroll := Value; end;
procedure TDataSetSnapshot._SetAutoCalcFields(const Value: Boolean); begin FData.AutoCalcFields := Value; end;
procedure TDataSetSnapshot._SetBeforeCancel(const Value: TDataSetNotifyEvent); begin FData.BeforeCancel := Value; end;
procedure TDataSetSnapshot._SetBeforeClose(const Value: TDataSetNotifyEvent); begin FData.BeforeClose := Value; end;
procedure TDataSetSnapshot._SetBeforeDelete(const Value: TDataSetNotifyEvent); begin FData.BeforeDelete := Value; end;
procedure TDataSetSnapshot._SetBeforeEdit(const Value: TDataSetNotifyEvent); begin FData.BeforeEdit := Value; end;
procedure TDataSetSnapshot._SetBeforeInsert(const Value: TDataSetNotifyEvent); begin FData.BeforeInsert := Value; end;
procedure TDataSetSnapshot._SetBeforeOpen(const Value: TDataSetNotifyEvent); begin FData.BeforeOpen := Value; end;
procedure TDataSetSnapshot._SetBeforePost(const Value: TDataSetNotifyEvent); begin FData.BeforePost := Value; end;
procedure TDataSetSnapshot._SetBeforeRefresh(const Value: TDataSetNotifyEvent); begin FData.BeforeRefresh := Value; end;
procedure TDataSetSnapshot._SetBeforeScroll(const Value: TDataSetNotifyEvent); begin FData.BeforeScroll := Value; end;
procedure TDataSetSnapshot._SetBlockReadSize(const Value: Integer); begin FData.BlockReadSize := Value; end;
procedure TDataSetSnapshot._SetBookmark(const Value: TBookmark); begin FData.Bookmark := Value; end;
procedure TDataSetSnapshot._SetCachedUpdates(const Value: Boolean); begin end;
procedure TDataSetSnapshot._SetCommandText(const ACommandText: String); begin end;
procedure TDataSetSnapshot._SetFetchingAll(const Value: Boolean); begin end;
function TDataSetSnapshot._GetFetchOptions: TFetchOptions; begin Result := TFetchOptions.Create(fmAll); end; 
procedure TDataSetSnapshot._SetFetchOptions(const Value: TFetchOptions); begin end;

procedure TDataSetSnapshot._SetFieldValue(const FieldName: string; const Value: Variant); begin FData.FieldValues[FieldName] := Value; end;
procedure TDataSetSnapshot._SetFilter(const Value: String); begin FData.Filter := Value; end;
procedure TDataSetSnapshot._SetFiltered(const Value: Boolean); begin FData.Filtered := Value; end;
procedure TDataSetSnapshot._SetFilterOptions(Value: TFilterOptions); begin FData.FilterOptions := Value; end;
procedure TDataSetSnapshot._SetOnCalcFields(const Value: TDataSetNotifyEvent); begin FData.OnCalcFields := Value; end;
procedure TDataSetSnapshot._SetOnDeleteError(const Value: TDataSetErrorEvent); begin FData.OnDeleteError := Value; end;
procedure TDataSetSnapshot._SetOnEditError(const Value: TDataSetErrorEvent); begin FData.OnEditError := Value; end;
procedure TDataSetSnapshot._SetOnFilterRecord(const Value: TFilterRecordEvent); begin FData.OnFilterRecord := Value; end;
procedure TDataSetSnapshot._SetOnNewRecord(const Value: TDataSetNotifyEvent); begin FData.OnNewRecord := Value; end;
procedure TDataSetSnapshot._SetOnPostError(const Value: TDataSetErrorEvent); begin FData.OnPostError := Value; end;
procedure TDataSetSnapshot._SetReadOnly(const Value: Boolean); begin end;
procedure TDataSetSnapshot._SetSortFields(const Value: String); begin end;
procedure TDataSetSnapshot._SetUniDirectional(const Value: Boolean); begin end;

end.
