@echo off
REM �N�x�����L���O�i���Ӑ�j�f�[�^�쐬�o�b�`�t�@�C��

REM �G���[�n���h�����O��L����
setlocal enabledelayedexpansion

REM Python�X�N���v�g�̎��s
python 01_�f�[�^�쐬.py
if %errorlevel% neq 0 (
    echo �G���[: 01_�f�[�^�쐬.py�̎��s�Ɏ��s���܂���
    pause
    exit /b 1
)

python 02_�ߋ��f�[�^����.py
if %errorlevel% neq 0 (
    echo �G���[: 02_�ߋ��f�[�^����.py�̎��s�Ɏ��s���܂���
    pause
    exit /b 1
)

python 03_�f�[�^�]�L.py
if %errorlevel% neq 0 (
    echo �G���[: 03_�f�[�^�]�L.py�̎��s�Ɏ��s���܂���
    pause
    exit /b 1
)

echo �S�Ă̏���������Ɋ������܂���
exit