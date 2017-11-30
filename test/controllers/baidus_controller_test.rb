require 'test_helper'

class BaidusControllerTest < ActionController::TestCase
  setup do
    @baidu = baidus(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:baidus)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create baidu" do
    assert_difference('Baidu.count') do
      post :create, baidu: {  }
    end

    assert_redirected_to baidu_path(assigns(:baidu))
  end

  test "should show baidu" do
    get :show, id: @baidu
    assert_response :success
  end

  test "should get edit" do
    get :edit, id: @baidu
    assert_response :success
  end

  test "should update baidu" do
    patch :update, id: @baidu, baidu: {  }
    assert_redirected_to baidu_path(assigns(:baidu))
  end

  test "should destroy baidu" do
    assert_difference('Baidu.count', -1) do
      delete :destroy, id: @baidu
    end

    assert_redirected_to baidus_path
  end
end
