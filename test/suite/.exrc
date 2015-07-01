if &cp | set nocp | endif
let s:cpo_save=&cpo
set cpo&vim
inoremap <silent> <S-Tab> =BackwardsSnippet()
vnoremap <silent>  :call RangeCommentLine()
nnoremap <silent>  :call CommentLine()
onoremap <silent>  :call CommentLine()
snoremap <silent> 	 i<Right>=TriggerSnippet()
snoremap  b<BS>
xnoremap <silent>  :call RangeUnCommentLine()
nnoremap <silent>  :call UnCommentLine()
onoremap <silent>  :call UnCommentLine()
snoremap % b<BS>%
snoremap ' b<BS>'
snoremap U b<BS>U
snoremap \ b<BS>\
nmap \ihn :IHN
nmap \is :IHS:A
nmap \ih :IHS
snoremap ^ b<BS>^
snoremap ` b<BS>`
vmap gx <Plug>NetrwBrowseXVis
nmap gx <Plug>NetrwBrowseX
snoremap <Left> bi
snoremap <Right> a
snoremap <BS> b<BS>
snoremap <silent> <S-Tab> i<Right>=BackwardsSnippet()
vnoremap <silent> <Plug>NetrwBrowseXVis :call netrw#BrowseXVis()
nnoremap <silent> <Plug>NetrwBrowseX :call netrw#BrowseX(expand((exists("g:netrw_gx")? g:netrw_gx : '<cfile>')),netrw#CheckIfRemote())
inoremap <silent> 	 =TriggerSnippet()
inoremap <silent> 	 =ShowAvailableSnips()
imap \ihn :IHN
imap \is :IHS:A
imap \ih :IHS
let &cpo=s:cpo_save
unlet s:cpo_save
set backspace=indent,eol,start
set cindent
set cscopeprg=/usr/bin/cscope
set cscopetag
set cscopeverbose
set fileencodings=ucs-bom,utf-8,default,latin1
set helplang=en
set nomodeline
set printoptions=paper:letter
set ruler
set runtimepath=~/.vim,/var/lib/vim/addons,/usr/share/vim/vimfiles,/usr/share/vim/vim74,/usr/share/vim/vimfiles/after,/var/lib/vim/addons/after,~/.vim/after
set shiftwidth=4
set suffixes=.bak,~,.swp,.o,.info,.aux,.log,.dvi,.bbl,.blg,.brf,.cb,.ind,.idx,.ilg,.inx,.out,.toc
set tabstop=4
" vim: set ft=vim :
